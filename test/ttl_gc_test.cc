#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/snapshot.h"
#include "gtest/gtest.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"

namespace leveldb {
namespace {

constexpr uint64_t kSecond = 1000000;
constexpr uint64_t kEpoch = 1700000000000000ULL;
std::string executable;

class GCEnv : public EnvWrapper {
 public:
  GCEnv() : EnvWrapper(Env::Default()) {}
  uint64_t NowMicros() override { return now.load(); }
  Status NewWritableFile(const std::string& path, WritableFile** result) override {
    uint64_t number;
    FileType type;
    const std::string basename = path.substr(path.find_last_of('/') + 1);
    if (on_table_create && ParseFileName(basename, &number, &type) &&
        type == kTableFile) {
      auto hook = std::move(on_table_create);
      on_table_create = nullptr;
      hook();
    }
    return target()->NewWritableFile(path, result);
  }
  std::function<void()> on_table_create;
  void Schedule(void (*function)(void*), void* arg) override {
    auto* task = new Task{this, function, arg};
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ++pending_;
    }
    target()->Schedule([](void* raw) {
      std::unique_ptr<Task> task(static_cast<Task*>(raw));
      task->function(task->arg);
      std::lock_guard<std::mutex> lock(task->env->mutex_);
      --task->env->pending_;
      task->env->finished_.notify_all();
    }, task);
  }
  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    finished_.wait(lock, [this] { return pending_ == 0; });
  }
  std::atomic<uint64_t> now{kEpoch};

 private:
  struct Task {
    GCEnv* env;
    void (*function)(void*);
    void* arg;
  };
  std::mutex mutex_;
  std::condition_variable finished_;
  size_t pending_ = 0;
};

class TTLGCTest : public testing::Test {
 protected:
  void SetUp() override {
    std::string dir;
    ASSERT_TRUE(env_.GetTestDirectory(&dir).ok());
    const auto* test = testing::UnitTest::GetInstance()->current_test_info();
    name_ = dir + "/ttl-gc-" + test->name() + "-" +
            std::to_string(Env::Default()->NowMicros());
    options_.create_if_missing = true;
    options_.env = &env_;
    options_.compression = kNoCompression;
    options_.write_buffer_size = 64 << 10;
    options_.max_file_size = 64 << 10;
    ASSERT_TRUE(DestroyDB(name_, options_).ok());
    Open();
    ASSERT_NE(db_, nullptr);
  }

  void TearDown() override {
    db_.reset();
    env_.Wait();
    EXPECT_TRUE(DestroyDB(name_, options_).ok());
  }

  void Open() {
    DB* raw = nullptr;
    const Status s = DB::Open(options_, name_, &raw);
    ASSERT_TRUE(s.ok()) << s.ToString();
    db_.reset(raw);
  }
  void Reopen() {
    db_.reset();
    env_.Wait();
    options_.create_if_missing = false;
    Open();
  }
  DBImpl* Impl() { return static_cast<DBImpl*>(db_.get()); }
  void Flush() {
    const Status s = Impl()->TEST_CompactMemTable();
    ASSERT_TRUE(s.ok()) << s.ToString();
    env_.Wait();
  }
  void Deepen() {
    Flush();
    for (int level = 0; level < config::kNumLevels - 1; ++level) {
      Impl()->TEST_CompactRange(level, nullptr, nullptr);
    }
    env_.Wait();
  }
  void GC() {
    db_->CompactRange(nullptr, nullptr);
    env_.Wait();
  }
  uint64_t Sequence() {
    const Snapshot* snapshot = db_->GetSnapshot();
    const uint64_t seq =
        static_cast<const SnapshotImpl*>(snapshot)->sequence_number();
    db_->ReleaseSnapshot(snapshot);
    return seq;
  }
  uint64_t PhysicalCount() {
    env_.Wait();
    std::unique_ptr<Iterator> it(Impl()->TEST_NewInternalIterator());
    uint64_t count = 0;
    std::set<std::string> keys;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ParsedInternalKey key;
      EXPECT_TRUE(ParseInternalKey(it->key(), &key));
      keys.insert(key.user_key.ToString());
      ++count;
    }
    EXPECT_TRUE(it->status().ok()) << it->status().ToString();
    Stats stats;
    EXPECT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
    EXPECT_EQ(count, stats.total_entries);
    EXPECT_EQ(keys.size(), stats.distinct_keys);
    EXPECT_EQ(Sequence(), stats.seq);
    return count;
  }
  std::set<std::string> TableFiles() {
    env_.Wait();
    std::vector<std::string> files;
    EXPECT_TRUE(env_.GetChildren(name_, &files).ok());
    std::set<std::string> tables;
    for (const auto& file : files) {
      uint64_t number;
      FileType type;
      if (ParseFileName(file, &number, &type) && type == kTableFile) {
        tables.insert(file);
      }
    }
    return tables;
  }
  uint64_t LevelFiles(int level) {
    std::string result;
    EXPECT_TRUE(db_->GetProperty("leveldb.num-files-at-level" +
                                     std::to_string(level), &result));
    return std::stoull(result);
  }
  void Value(const std::string& key, const std::string& expected,
             const Snapshot* snapshot = nullptr) {
    ReadOptions read;
    read.snapshot = snapshot;
    std::string value;
    const Status s = db_->Get(read, key, &value);
    ASSERT_TRUE(s.ok()) << s.ToString();
    EXPECT_EQ(expected, value);
  }
  void Missing(const std::string& key) {
    std::string value;
    const Status s = db_->Get(ReadOptions(), key, &value);
    EXPECT_TRUE(s.IsNotFound()) << s.ToString();
  }
  void IteratorValues(Iterator* it,
                      const std::map<std::string, std::string>& expected) {
    auto next = expected.begin();
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ASSERT_NE(next, expected.end());
      EXPECT_EQ(next->first, it->key().ToString());
      EXPECT_EQ(next->second, it->value().ToString());
      ++next;
    }
    EXPECT_EQ(next, expected.end());
    auto previous = expected.rbegin();
    for (it->SeekToLast(); it->Valid(); it->Prev()) {
      ASSERT_NE(previous, expected.rend());
      EXPECT_EQ(previous->first, it->key().ToString());
      EXPECT_EQ(previous->second, it->value().ToString());
      ++previous;
    }
    EXPECT_EQ(previous, expected.rend());
    EXPECT_TRUE(it->status().ok());
    for (const auto& item : expected) {
      it->Seek(item.first);
      ASSERT_TRUE(it->Valid());
      EXPECT_EQ(item.first, it->key().ToString());
      EXPECT_EQ(item.second, it->value().ToString());
    }
  }

  GCEnv env_;
  Options options_;
  std::string name_;
  std::unique_ptr<DB> db_;
};

TEST_F(TTLGCTest, ExpiryAloneDoesNotRemovePhysicalRecords) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "ttl", "x", 1).ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "plain", "y").ok());
  Flush();
  EXPECT_EQ(2, PhysicalCount());
  env_.now += kSecond;
  Missing("ttl");
  EXPECT_EQ(2, PhysicalCount());
  GC();
  EXPECT_EQ(1, PhysicalCount());
  EXPECT_EQ(2, Sequence());
  Value("plain", "y");
  Reopen();
  EXPECT_EQ(1, PhysicalCount());
  EXPECT_EQ(2, Sequence());
}

TEST_F(TTLGCTest, PublicCompactRangeCollectsNewlyFlushedRecords) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "zero", "x", 0).ok());
  EXPECT_EQ(1, PhysicalCount());
  GC();
  EXPECT_EQ(0, PhysicalCount());
  EXPECT_EQ(1, Sequence());
  EXPECT_TRUE(TableFiles().empty());
  GC();
  Reopen();
  EXPECT_EQ(0, PhysicalCount());
  EXPECT_EQ(1, Sequence());
}

TEST_F(TTLGCTest, DeepestLevelRewritesAllSelectedFilesAndIsRepeatable) {
  std::map<std::string, std::string> kept;
  const std::string payload(32768, 'x');
  for (int i = 0; i < 96; ++i) {
    const std::string key = "key/" + std::to_string(1000 + i);
    ASSERT_TRUE(db_->Put(WriteOptions(), key, payload, i < 48 ? 1 : 100).ok());
    if (i >= 48) kept[key] = payload;
  }
  Deepen();
  ASSERT_GT(LevelFiles(config::kNumLevels - 1), 1);
  EXPECT_EQ(96, PhysicalCount());
  env_.now += kSecond;
  Impl()->TEST_CompactRange(config::kNumLevels - 1, nullptr, nullptr);
  env_.Wait();
  EXPECT_EQ(48, PhysicalCount());
  EXPECT_EQ(96, Sequence());
  std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
  IteratorValues(it.get(), kept);
  it.reset();
  Impl()->TEST_CompactRange(config::kNumLevels - 1, nullptr, nullptr);
  env_.Wait();
  EXPECT_EQ(48, PhysicalCount());
  Reopen();
  EXPECT_EQ(48, PhysicalCount());
  env_.now += 99 * kSecond;
  GC();
  EXPECT_EQ(0, PhysicalCount());
  EXPECT_EQ(96, Sequence());
  EXPECT_TRUE(TableFiles().empty());
  Reopen();
  EXPECT_EQ(0, PhysicalCount());
  EXPECT_EQ(96, Sequence());
}

TEST_F(TTLGCTest, ExpiredBarrierSurvivesUntilDeeperOldValueIsMerged) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "old").ok());
  Deepen();
  ASSERT_EQ(1, LevelFiles(config::kNumLevels - 1));
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "new", 1).ok());
  Flush();
  env_.now += kSecond;
  Impl()->TEST_CompactRange(config::kMaxMemCompactLevel, nullptr, nullptr, true);
  env_.Wait();
  EXPECT_EQ(2, PhysicalCount());
  Missing("a");
  GC();
  EXPECT_EQ(0, PhysicalCount());
  Missing("a");
  EXPECT_EQ(2, Sequence());
  Reopen();
  Missing("a");
  EXPECT_EQ(0, PhysicalCount());
}

TEST_F(TTLGCTest, SnapshotBeforeExpiryProtectsThenReleaseAllowsCollection) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "ttl", 1).ok());
  Deepen();
  const Snapshot* before = db_->GetSnapshot();
  env_.now += kSecond;
  const Snapshot* after = db_->GetSnapshot();
  GC();
  EXPECT_EQ(1, PhysicalCount());
  Value("a", "ttl", before);
  Missing("a");
  ReadOptions read;
  read.snapshot = before;
  std::unique_ptr<Iterator> pinned(db_->NewIterator(read));
  db_->ReleaseSnapshot(before);
  GC();
  EXPECT_EQ(0, PhysicalCount());
  IteratorValues(pinned.get(), {{"a", "ttl"}});
  std::string value;
  read.snapshot = after;
  EXPECT_TRUE(db_->Get(read, "a", &value).IsNotFound());
  db_->ReleaseSnapshot(after);
  pinned.reset();
  GC();
  EXPECT_EQ(0, PhysicalCount());
  EXPECT_TRUE(TableFiles().empty());
  EXPECT_EQ(1, Sequence());
}

TEST_F(TTLGCTest, LazyIteratorPinsObsoleteSSTAfterCurrentVersionIsEmpty) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "ttl", 1).ok());
  Deepen();
  const auto original = TableFiles();
  ASSERT_FALSE(original.empty());
  std::unique_ptr<Iterator> pinned(db_->NewIterator(ReadOptions()));
  env_.now += kSecond;
  GC();
  EXPECT_EQ(0, PhysicalCount());
  Missing("a");
  const auto files = TableFiles();
  for (const auto& file : original) EXPECT_EQ(1, files.count(file));
  IteratorValues(pinned.get(), {{"a", "ttl"}});
  pinned.reset();
  GC();
  EXPECT_TRUE(TableFiles().empty());
  Reopen();
  EXPECT_EQ(0, PhysicalCount());
  EXPECT_EQ(1, Sequence());
}

TEST_F(TTLGCTest, SnapshotPreservesOldNormalVersionBehindExpiredOverwrite) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "old").ok());
  const Snapshot* old = db_->GetSnapshot();
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "expired", 0).ok());
  GC();
  Missing("a");
  Value("a", "old", old);
  EXPECT_EQ(2, PhysicalCount());
  db_->ReleaseSnapshot(old);
  GC();
  EXPECT_EQ(0, PhysicalCount());
  EXPECT_EQ(2, Sequence());
}

TEST_F(TTLGCTest, TTLOverwriteDeletesAndBatchLeaveExactSurvivors) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "old").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "ttl", 1).ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "b", "ttl", 1).ok());
  WriteBatch batch;
  batch.Put("b", "plain");
  batch.Put("c", "discard");
  batch.Delete("c");
  batch.Delete("d");
  ASSERT_TRUE(db_->Write(WriteOptions(), &batch).ok());
  env_.now += kSecond;
  GC();
  EXPECT_EQ(1, PhysicalCount());
  EXPECT_EQ(7, Sequence());
  Value("b", "plain");
  Missing("a");
  Missing("c");
  Missing("d");
  Reopen();
  EXPECT_EQ(1, PhysicalCount());
  EXPECT_EQ(7, Sequence());
}

TEST_F(TTLGCTest, L0RewriteRequestNeverReordersVersionsByFileNumber) {
  options_.write_buffer_size = 8 << 20;
  options_.max_file_size = 1 << 20;
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "seed").ok());
  Reopen();
  ASSERT_GT(LevelFiles(0), 0);
  const Snapshot* old = db_->GetSnapshot();
  const std::string first(600 << 10, 'x');
  const std::string second(600 << 10, 'y');
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", first).ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", second).ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", first, 0).ok());
  Flush();
  Impl()->TEST_CompactRange(0, nullptr, nullptr, true);
  env_.Wait();
  Missing("a");
  Value("a", "seed", old);
  std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
  IteratorValues(it.get(), {});
  it.reset();
  EXPECT_EQ(0, LevelFiles(0));
  db_->ReleaseSnapshot(old);
  GC();
  EXPECT_EQ(0, PhysicalCount());
  EXPECT_EQ(4, Sequence());
}

TEST_F(TTLGCTest, SnapshotCreatedDuringClockRollbackPreventsInstallation) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "ttl", 1).ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "z", "plain").ok());
  Deepen();
  env_.now = kEpoch + 2 * kSecond;
  const Snapshot* snapshot = nullptr;
  env_.on_table_create = [&] {
    env_.now = kEpoch + kSecond / 2;
    snapshot = db_->GetSnapshot();
    Value("a", "ttl", snapshot);
  };
  GC();
  ASSERT_NE(snapshot, nullptr);
  Value("a", "ttl", snapshot);
  EXPECT_EQ(2, PhysicalCount());
  env_.now = kEpoch + 2 * kSecond;
  Missing("a");
  GC();
  Value("a", "ttl", snapshot);
  EXPECT_EQ(2, PhysicalCount());
  db_->ReleaseSnapshot(snapshot);
  GC();
  EXPECT_EQ(1, PhysicalCount());
  EXPECT_EQ(2, Sequence());
  Value("z", "plain");
}

TEST_F(TTLGCTest, PartialRangeRewritesOnlyOverlappingDeepFiles) {
  const std::string payload(32768, 'x');
  for (int i = 0; i < 48; ++i) {
    ASSERT_TRUE(db_->Put(WriteOptions(), "key/" + std::to_string(1000 + i),
                         payload, 1).ok());
  }
  Deepen();
  env_.now += kSecond;
  Slice begin("key/1020"), end("key/1025");
  db_->CompactRange(&begin, &end);
  env_.Wait();
  EXPECT_GT(PhysicalCount(), 0);
  EXPECT_LT(PhysicalCount(), 48);
  for (int i = 20; i <= 25; ++i) Missing("key/" + std::to_string(1000 + i));
  GC();
  EXPECT_EQ(0, PhysicalCount());
  EXPECT_EQ(48, Sequence());
}

TEST_F(TTLGCTest, ConcurrentCompactionAndTTLBatchWritesKeepPinnedView) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "pinned", "original", 1).ok());
  Flush();
  const Snapshot* view = db_->GetSnapshot();
  std::unique_ptr<Iterator> pinned(db_->NewIterator(ReadOptions()));
  env_.now += kSecond;
  std::atomic<bool> failed{false};
  std::thread compactor([&] {
    for (int i = 0; i < 8; ++i) db_->CompactRange(nullptr, nullptr);
  });
  std::vector<std::thread> writers;
  for (int thread = 0; thread < 4; ++thread) {
    writers.emplace_back([&, thread] {
      for (int i = 0; i < 100; ++i) {
        const std::string key = std::to_string(thread) + "/" + std::to_string(i);
        if (!db_->Put(WriteOptions(), key, "expired", 0).ok()) failed = true;
        WriteBatch batch;
        batch.Put(key + "/live", "live");
        batch.Put(key + "/gone", "gone");
        batch.Delete(key + "/gone");
        if (!db_->Write(WriteOptions(), &batch).ok()) failed = true;
        std::string value;
        if (!db_->Get(ReadOptions(), key, &value).IsNotFound()) failed = true;
      }
    });
  }
  for (auto& writer : writers) writer.join();
  compactor.join();
  EXPECT_FALSE(failed.load());
  Value("pinned", "original", view);
  IteratorValues(pinned.get(), {{"pinned", "original"}});
  db_->ReleaseSnapshot(view);
  pinned.reset();
  GC();
  EXPECT_EQ(400, PhysicalCount());
  EXPECT_EQ(1601, Sequence());
  Reopen();
  EXPECT_EQ(400, PhysicalCount());
  EXPECT_EQ(1601, Sequence());
}

#if defined(__unix__) || defined(__APPLE__)
TEST_F(TTLGCTest, SyncWALSurvivesAbruptProcessExitAndRemainsCollectible) {
  db_.reset();
  env_.Wait();
  const pid_t pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    execl(executable.c_str(), executable.c_str(), "--crash-write",
          name_.c_str(), static_cast<char*>(nullptr));
    _exit(127);
  }
  int status = 0;
  ASSERT_EQ(pid, waitpid(pid, &status, 0));
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(0, WEXITSTATUS(status));
  options_.create_if_missing = false;
  Open();
  Value("ttl", "synced");
  Value("plain", "synced");
  Missing("deleted");
  EXPECT_EQ(3, PhysicalCount());
  EXPECT_EQ(3, Sequence());
  env_.now += kSecond;
  Missing("ttl");
  GC();
  EXPECT_EQ(1, PhysicalCount());
  EXPECT_EQ(3, Sequence());
  Reopen();
  EXPECT_EQ(1, PhysicalCount());
  EXPECT_EQ(3, Sequence());
}
#endif

int CrashWrite(const std::string& path) {
  GCEnv env;
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  DB* db = nullptr;
  if (!DB::Open(options, path, &db).ok()) return 2;
  WriteOptions write;
  write.sync = true;
  if (!db->Put(write, "ttl", "synced", 1).ok()) return 3;
  if (!db->Put(write, "plain", "synced").ok()) return 4;
  if (!db->Delete(write, "deleted").ok()) return 5;
#if defined(__unix__) || defined(__APPLE__)
  // No DB destructor or normal close: success confirms all sync writes completed.
  _exit(0);
#endif
  return 0;
}

}  // namespace
}  // namespace leveldb

int main(int argc, char** argv) {
  if (argc == 3 && std::string(argv[1]) == "--crash-write") {
    return leveldb::CrashWrite(argv[2]);
  }
  leveldb::executable = argv[0];
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
