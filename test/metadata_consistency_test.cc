#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "db/version_edit.h"
#include "db/write_batch_internal.h"
#include "gtest/gtest.h"
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"

namespace leveldb {
namespace {

constexpr uint64_t kEpoch = 1700000000000000ULL;
constexpr uint64_t kSecond = 1000000;

class StatsEnv : public EnvWrapper {
 public:
  StatsEnv() : EnvWrapper(Env::Default()) {}
  uint64_t NowMicros() override { return now.load(); }

  class File : public WritableFile {
   public:
    File(WritableFile* file, StatsEnv* env, FileType type)
        : file_(file), env_(env), type_(type), log_appends_(0) {}
    Status Append(const Slice& data) override {
      if (type_ == kDescriptorFile && env_->fail_manifest_append) {
        return Status::IOError("injected manifest append failure");
      }
      if (type_ == kLogFile && log_appends_++ == 1 &&
          env_->fail_log_payload_append.exchange(false)) {
        return Status::IOError("injected partial WAL append failure");
      }
      return file_->Append(data);
    }
    Status Close() override { return file_->Close(); }
    Status Flush() override { return file_->Flush(); }
    Status Sync() override {
      if (type_ == kTableFile && env_->fail_table_sync) {
        return Status::IOError("injected SST sync failure");
      }
      return file_->Sync();
    }
   private:
    std::unique_ptr<WritableFile> file_;
    StatsEnv* env_;
    FileType type_;
    int log_appends_;
  };

  Status NewWritableFile(const std::string& path, WritableFile** result) override {
    uint64_t number = 0;
    FileType type = kTempFile;
    ParseFileName(path.substr(path.find_last_of('/') + 1), &number, &type);
    if (type == kTableFile) {
      std::unique_lock<std::mutex> lock(mutex_);
      if (block_table_) {
        blocked_ = true;
        changed_.notify_all();
        changed_.wait(lock, [this] { return !block_table_; });
      }
    }
    WritableFile* file = nullptr;
    Status status = target()->NewWritableFile(path, &file);
    *result = status.ok() ? new File(file, this, type) : nullptr;
    return status;
  }

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
      task->env->changed_.notify_all();
    }, task);
  }
  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    changed_.wait(lock, [this] { return pending_ == 0; });
  }
  void BlockTable() {
    std::lock_guard<std::mutex> lock(mutex_);
    blocked_ = false;
    block_table_ = true;
  }
  bool AwaitBlocked() {
    std::unique_lock<std::mutex> lock(mutex_);
    return changed_.wait_for(lock, std::chrono::seconds(10),
                             [this] { return blocked_; });
  }
  void UnblockTable() {
    std::lock_guard<std::mutex> lock(mutex_);
    block_table_ = false;
    changed_.notify_all();
  }
  std::atomic<uint64_t> now{kEpoch};
  std::atomic<bool> fail_manifest_append{false};
  std::atomic<bool> fail_log_payload_append{false};
  std::atomic<bool> fail_table_sync{false};

 private:
  struct Task {
    StatsEnv* env;
    void (*function)(void*);
    void* arg;
  };
  std::mutex mutex_;
  std::condition_variable changed_;
  size_t pending_ = 0;
  bool block_table_ = false;
  bool blocked_ = false;
};

struct KeyLess {
  const Comparator* comparator;
  bool operator()(const std::string& a, const std::string& b) const {
    return comparator->Compare(a, b) < 0;
  }
};

class MetadataConsistencyTest : public testing::Test {
 protected:
  void SetUp() override {
    std::string dir;
    ASSERT_TRUE(env_.GetTestDirectory(&dir).ok());
    const auto* test = testing::UnitTest::GetInstance()->current_test_info();
    name_ = dir + "/metadata-consistency-" + test->name() + "-" +
            std::to_string(Env::Default()->NowMicros());
    options_.env = &env_;
    options_.create_if_missing = true;
    options_.compression = kNoCompression;
    ASSERT_TRUE(DestroyDB(name_, options_).ok());
    Open();
    ASSERT_NE(db_, nullptr);
  }
  void TearDown() override {
    env_.UnblockTable();
    db_.reset();
    env_.Wait();
    EXPECT_TRUE(DestroyDB(name_, options_).ok());
  }
  void Open() {
    DB* raw = nullptr;
    Status status = DB::Open(options_, name_, &raw);
    ASSERT_TRUE(status.ok()) << status.ToString();
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
    const Status status = Impl()->TEST_CompactMemTable();
    ASSERT_TRUE(status.ok()) << status.ToString();
    env_.Wait();
  }
  void GC() {
    db_->CompactRange(nullptr, nullptr);
    env_.Wait();
  }
  void Expected(uint64_t total, uint64_t distinct, uint64_t sequence,
                const Snapshot* snapshot = nullptr) {
    Stats stats;
    ReadOptions read;
    read.snapshot = snapshot;
    ASSERT_TRUE(db_->GetCurrentStats(read, &stats).ok());
    EXPECT_EQ(total, stats.total_entries);
    EXPECT_EQ(distinct, stats.distinct_keys);
    EXPECT_EQ(sequence, stats.seq);
  }
  void PhysicalOracle(uint64_t sequence) {
    env_.Wait();
    std::unique_ptr<Iterator> it(Impl()->TEST_NewInternalIterator());
    uint64_t total = 0;
    std::set<std::string, KeyLess> keys(KeyLess{options_.comparator});
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ParsedInternalKey key;
      ASSERT_TRUE(ParseInternalKey(it->key(), &key));
      keys.insert(key.user_key.ToString());
      ++total;
    }
    ASSERT_TRUE(it->status().ok()) << it->status().ToString();
    Expected(total, keys.size(), sequence);
  }
  StatsEnv env_;
  Options options_;
  std::string name_;
  std::unique_ptr<DB> db_;
};

TEST_F(MetadataConsistencyTest, StepwiseOperationsBatchAndIgnoredSnapshot) {
  Expected(0, 0, 0);
  EXPECT_TRUE(db_->GetCurrentStats(ReadOptions(), nullptr).IsInvalidArgument());
  const Snapshot* empty = db_->GetSnapshot();
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "1").ok());
  Expected(1, 1, 1);
  ASSERT_TRUE(db_->Put(WriteOptions(), "b", "1").ok());
  Expected(2, 2, 2);
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "2").ok());
  Expected(3, 2, 3);
  ASSERT_TRUE(db_->Delete(WriteOptions(), "b").ok());
  Expected(4, 2, 4);
  WriteBatch batch;
  batch.Put("a", "3");
  batch.Delete("missing");
  batch.Put("c", "1");
  batch.Delete("c");
  ASSERT_TRUE(db_->Write(WriteOptions(), &batch).ok());
  Expected(8, 4, 8, empty);
  PhysicalOracle(8);
  batch.Clear();
  ASSERT_TRUE(db_->Write(WriteOptions(), &batch).ok());
  Expected(8, 4, 8);
  db_->ReleaseSnapshot(empty);
  GC();
  Expected(1, 1, 8);
  PhysicalOracle(8);
}

TEST_F(MetadataConsistencyTest, FailedWALAppendDoesNotPublishSequenceOrStats) {
  WriteBatch batch;
  batch.Put("not-published", "value");
  env_.fail_log_payload_append = true;
  EXPECT_TRUE(db_->Write(WriteOptions(), &batch).IsIOError());
  Expected(0, 0, 0);

  EXPECT_TRUE(db_->Put(WriteOptions(), "must-not-append", "value").IsIOError());
  Expected(0, 0, 0);

  options_.reuse_logs = true;
  Reopen();
  Expected(0, 0, 0);
  std::string value;
  EXPECT_TRUE(db_->Get(ReadOptions(), "not-published", &value).IsNotFound());
  ASSERT_TRUE(db_->Put(WriteOptions(), "published", "value").ok());
  Expected(1, 1, 1);
  Reopen();
  Expected(1, 1, 1);
  ASSERT_TRUE(db_->Get(ReadOptions(), "published", &value).ok());
  EXPECT_EQ("value", value);
}

TEST_F(MetadataConsistencyTest, SequenceExhaustionAndEmptyBatchDoNotWrap) {
  Impl()->TEST_SetLastSequence(kMaxSequenceNumber - 1);
  ASSERT_TRUE(db_->Put(WriteOptions(), "last", "value").ok());
  Expected(1, 1, kMaxSequenceNumber);

  EXPECT_TRUE(db_->Put(WriteOptions(), "overflow", "value").IsInvalidArgument());
  Expected(1, 1, kMaxSequenceNumber);
  std::string value;
  EXPECT_TRUE(db_->Get(ReadOptions(), "overflow", &value).IsNotFound());

  WriteBatch empty;
  ASSERT_TRUE(db_->Write(WriteOptions(), &empty).ok());
  Reopen();
  Expected(1, 1, kMaxSequenceNumber);
}

TEST_F(MetadataConsistencyTest, FutureDatabaseFormatIsRejected) {
  db_.reset();
  env_.Wait();

  std::string current;
  ASSERT_TRUE(ReadFileToString(&env_, CurrentFileName(name_), &current).ok());
  ASSERT_FALSE(current.empty());
  ASSERT_EQ('\n', current.back());
  current.pop_back();
  const std::string manifest = name_ + "/" + current;

  uint64_t manifest_size = 0;
  ASSERT_TRUE(env_.GetFileSize(manifest, &manifest_size).ok());
  WritableFile* file = nullptr;
  ASSERT_TRUE(env_.NewAppendableFile(manifest, &file).ok());
  log::Writer writer(file, manifest_size);
  VersionEdit edit;
  edit.SetFormatVersion(kCurrentDatabaseFormatVersion + 1);
  std::string record;
  edit.EncodeTo(&record);
  ASSERT_TRUE(writer.AddRecord(record).ok());
  ASSERT_TRUE(file->Sync().ok());
  ASSERT_TRUE(file->Close().ok());
  delete file;

  options_.create_if_missing = false;
  DB* unsupported = nullptr;
  const Status status = DB::Open(options_, name_, &unsupported);
  EXPECT_TRUE(status.IsNotSupportedError()) << status.ToString();
  EXPECT_EQ(nullptr, unsupported);
}

TEST_F(MetadataConsistencyTest, DatabaseFormatRegressionIsRejected) {
  db_.reset();
  env_.Wait();

  std::string current;
  ASSERT_TRUE(ReadFileToString(&env_, CurrentFileName(name_), &current).ok());
  ASSERT_FALSE(current.empty());
  ASSERT_EQ('\n', current.back());
  current.pop_back();
  const std::string manifest = name_ + "/" + current;

  uint64_t manifest_size = 0;
  ASSERT_TRUE(env_.GetFileSize(manifest, &manifest_size).ok());
  WritableFile* file = nullptr;
  ASSERT_TRUE(env_.NewAppendableFile(manifest, &file).ok());
  log::Writer writer(file, manifest_size);
  VersionEdit edit;
  edit.SetFormatVersion(kCurrentDatabaseFormatVersion - 1);
  std::string record;
  edit.EncodeTo(&record);
  ASSERT_TRUE(writer.AddRecord(record).ok());
  ASSERT_TRUE(file->Sync().ok());
  ASSERT_TRUE(file->Close().ok());
  delete file;

  options_.create_if_missing = false;
  DB* invalid = nullptr;
  const Status status = DB::Open(options_, name_, &invalid);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_EQ(nullptr, invalid);
}

TEST_F(MetadataConsistencyTest, LegacyManifestIsUpgradedOnOpen) {
  db_.reset();
  env_.Wait();

  std::string current;
  ASSERT_TRUE(ReadFileToString(&env_, CurrentFileName(name_), &current).ok());
  ASSERT_FALSE(current.empty());
  ASSERT_EQ('\n', current.back());
  current.pop_back();
  const std::string legacy_manifest_name = current;
  const std::string legacy_manifest = name_ + "/" + current;

  WritableFile* file = nullptr;
  ASSERT_TRUE(env_.NewWritableFile(legacy_manifest, &file).ok());
  {
    log::Writer writer(file);
    VersionEdit legacy;
    legacy.SetComparatorName(options_.comparator->Name());
    legacy.SetLogNumber(0);
    legacy.SetNextFile(2);
    legacy.SetLastSequence(0);
    std::string record;
    legacy.EncodeTo(&record);
    ASSERT_TRUE(writer.AddRecord(record).ok());
  }
  ASSERT_TRUE(file->Sync().ok());
  ASSERT_TRUE(file->Close().ok());
  delete file;

  options_.create_if_missing = false;
  options_.reuse_logs = true;
  Open();
  Expected(0, 0, 0);
  db_.reset();

  ASSERT_TRUE(ReadFileToString(&env_, CurrentFileName(name_), &current).ok());
  ASSERT_FALSE(current.empty());
  ASSERT_EQ('\n', current.back());
  current.pop_back();
  EXPECT_EQ(legacy_manifest_name, current);
  SequentialFile* manifest_file = nullptr;
  ASSERT_TRUE(
      env_.NewSequentialFile(name_ + "/" + current, &manifest_file).ok());
  bool found_format_version = false;
  {
    log::Reader reader(manifest_file, nullptr, true, 0);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch)) {
      VersionEdit edit;
      ASSERT_TRUE(edit.DecodeFrom(record).ok());
      if (edit.DebugString().find("FormatVersion:") != std::string::npos) {
        found_format_version = true;
      }
    }
    EXPECT_FALSE(reader.HasCorruptionOrTruncatedTail());
  }
  delete manifest_file;
  EXPECT_TRUE(found_format_version);
}

TEST_F(MetadataConsistencyTest, TTLExpiryAndInvalidDeadlineOnlyChangeOnGC) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "old").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "ttl", 1).ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "b", "zero", 0).ok());
  ASSERT_TRUE(db_->Delete(WriteOptions(), "c").ok());
  Expected(4, 3, 4);
  env_.now += kSecond;
  std::string value;
  EXPECT_TRUE(db_->Get(ReadOptions(), "a", &value).IsNotFound());
  Expected(4, 3, 4);
  EXPECT_TRUE(db_->Put(WriteOptions(), "overflow", "x",
                        std::numeric_limits<uint64_t>::max()).IsInvalidArgument());
  Expected(4, 3, 4);
  PhysicalOracle(4);
  GC();
  Expected(0, 0, 4);
  PhysicalOracle(4);
  Reopen();
  Expected(0, 0, 4);
}

TEST_F(MetadataConsistencyTest, KeysAreDeduplicatedAcrossMemoryAndSSTs) {
  const Snapshot* empty = db_->GetSnapshot();
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "1").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "b", "1").ok());
  Flush();
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "2").ok());
  ASSERT_TRUE(db_->Delete(WriteOptions(), "b").ok());
  Flush();
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "3").ok());
  ASSERT_TRUE(db_->Delete(WriteOptions(), "ghost").ok());
  Expected(6, 3, 6);
  PhysicalOracle(6);
  GC();
  Expected(6, 3, 6);
  PhysicalOracle(6);
  db_->ReleaseSnapshot(empty);
  GC();
  Expected(1, 1, 6);
  PhysicalOracle(6);
  Reopen();
  Expected(1, 1, 6);
}

TEST_F(MetadataConsistencyTest, ImmutableFlushTransferDoesNotDoubleCount) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "1").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "2").ok());
  env_.BlockTable();
  Status flushed;
  std::thread flush([&] { flushed = Impl()->TEST_CompactMemTable(); });
  const bool blocked = env_.AwaitBlocked();
  EXPECT_TRUE(blocked);
  if (blocked) {
    Expected(2, 1, 2);
    EXPECT_TRUE(db_->Put(WriteOptions(), "a", "3").ok());
    EXPECT_TRUE(db_->Put(WriteOptions(), "b", "1").ok());
    Expected(4, 2, 4);
    Stats stats;
    EXPECT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
  }
  env_.UnblockTable();
  flush.join();
  ASSERT_TRUE(flushed.ok()) << flushed.ToString();
  env_.Wait();
  Expected(4, 2, 4);
  PhysicalOracle(4);
}

TEST_F(MetadataConsistencyTest, BinaryKeysAndDeletesOfAbsentKeysCount) {
  const std::string binary("a\0b", 3);
  ASSERT_TRUE(db_->Put(WriteOptions(), "", "empty").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), binary, "binary", 1).ok());
  ASSERT_TRUE(db_->Delete(WriteOptions(), "absent").ok());
  ASSERT_TRUE(db_->Delete(WriteOptions(), "absent").ok());
  Expected(4, 3, 4);
  Flush();
  Reopen();
  Expected(4, 3, 4);
  PhysicalOracle(4);
  env_.now += kSecond;
  GC();
  Expected(1, 1, 4);
  PhysicalOracle(4);
}

class FoldComparator : public Comparator {
 public:
  const char* Name() const override { return "test.ascii-case-fold"; }
  int Compare(const Slice& a, const Slice& b) const override {
    const size_t n = a.size() < b.size() ? a.size() : b.size();
    for (size_t i = 0; i < n; ++i) {
      const unsigned char ac = Fold(a[i]), bc = Fold(b[i]);
      if (ac != bc) return ac < bc ? -1 : 1;
    }
    return a.size() == b.size() ? 0 : a.size() < b.size() ? -1 : 1;
  }
  void FindShortestSeparator(std::string*, const Slice&) const override {}
  void FindShortSuccessor(std::string*) const override {}
 private:
  static unsigned char Fold(unsigned char c) {
    return c >= 'A' && c <= 'Z' ? c + ('a' - 'A') : c;
  }
};

TEST_F(MetadataConsistencyTest, CustomComparatorDefinesDistinctKeys) {
  static FoldComparator comparator;
  db_.reset();
  env_.Wait();
  ASSERT_TRUE(DestroyDB(name_, options_).ok());
  options_.comparator = &comparator;
  Open();
  const Snapshot* empty = db_->GetSnapshot();
  ASSERT_TRUE(db_->Put(WriteOptions(), "A", "1").ok());
  Flush();
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "2").ok());
  ASSERT_TRUE(db_->Delete(WriteOptions(), "B").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "b", "3").ok());
  Expected(4, 2, 4);
  PhysicalOracle(4);
  db_->ReleaseSnapshot(empty);
  GC();
  Expected(2, 2, 4);
  Reopen();
  Expected(2, 2, 4);
  PhysicalOracle(4);
}

TEST_F(MetadataConsistencyTest, MalformedBatchNeverPublishesAPrefix) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "old", "1").ok());
  WriteBatch batch;
  batch.Put("would-be-prefix", "2");
  std::string raw = WriteBatchInternal::Contents(&batch).ToString();
  raw.push_back(static_cast<char>(kTypeTTLValue));
  PutLengthPrefixedSlice(&raw, "bad");
  PutLengthPrefixedSlice(&raw, "short");
  WriteBatchInternal::SetContents(&batch, raw);
  WriteBatchInternal::SetCount(&batch, 2);
  EXPECT_TRUE(db_->Write(WriteOptions(), &batch).IsCorruption());
  Expected(1, 1, 1);
  std::string value;
  EXPECT_TRUE(db_->Get(ReadOptions(), "would-be-prefix", &value).IsNotFound());
  PhysicalOracle(1);
  Reopen();
  Expected(1, 1, 1);
  ASSERT_TRUE(db_->Put(WriteOptions(), "next", "2").ok());
  Expected(2, 2, 2);
}

TEST_F(MetadataConsistencyTest, LogReuseModesRebuildFromActualRecoveredSources) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "1").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "2").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "3").ok());
  ASSERT_TRUE(db_->Delete(WriteOptions(), "ghost").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "ttl", "1", 1).ok());
  for (bool reuse : {false, true, false, true}) {
    options_.reuse_logs = reuse;
    Reopen();
    Expected(5, 3, 5);
    PhysicalOracle(5);
  }
  env_.now += kSecond;
  Expected(5, 3, 5);
  GC();
  Expected(1, 1, 5);
  for (bool reuse : {true, false}) {
    options_.reuse_logs = reuse;
    Reopen();
    Expected(1, 1, 5);
    PhysicalOracle(5);
  }
}

TEST_F(MetadataConsistencyTest, FailedSSTSyncDoesNotDeductUninstalledDrops) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "1").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "2").ok());
  Flush();
  Expected(2, 1, 2);
  env_.fail_table_sync = true;
  GC();
  Expected(2, 1, 2);
  PhysicalOracle(2);
  env_.fail_table_sync = false;
  Reopen();
  Expected(2, 1, 2);
  PhysicalOracle(2);
  GC();
  Expected(1, 1, 2);
}

TEST_F(MetadataConsistencyTest, FailedManifestAppendKeepsOldVersionCounts) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "1").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "2").ok());
  Flush();
  env_.fail_manifest_append = true;
  GC();
  Expected(2, 1, 2);
  PhysicalOracle(2);
  env_.fail_manifest_append = false;
  Reopen();
  Expected(2, 1, 2);
  GC();
  Expected(1, 1, 2);
  PhysicalOracle(2);
}

TEST_F(MetadataConsistencyTest, ConcurrentQueriesNeverSeeHalfABatch) {
  options_.write_buffer_size = 64 << 10;
  Reopen();
  const Snapshot* empty = db_->GetSnapshot();
  std::atomic<int> finished{0};
  std::atomic<bool> failed{false};
  std::vector<std::thread> writers;
  for (int thread = 0; thread < 4; ++thread) {
    writers.emplace_back([&, thread] {
      for (int i = 0; i < 200; ++i) {
        const std::string key = std::to_string(thread) + "/" + std::to_string(i);
        WriteBatch batch;
        batch.Put(key, "1");
        batch.Delete(key);
        batch.Put(key + "/live", "2");
        if (!db_->Write(WriteOptions(), &batch).ok()) failed = true;
      }
      ++finished;
    });
  }
  std::thread compactor([&] {
    for (int i = 0; i < 6; ++i) db_->CompactRange(nullptr, nullptr);
  });
  uint64_t previous = 0;
  do {
    Stats stats;
    if (!db_->GetCurrentStats(ReadOptions(), &stats).ok() ||
        stats.total_entries != stats.seq || stats.seq % 3 != 0 ||
        stats.distinct_keys != (stats.seq / 3) * 2 || stats.seq < previous) {
      failed = true;
    }
    previous = stats.seq;
    Env::Default()->SleepForMicroseconds(100);
  } while (finished != 4);
  for (auto& writer : writers) writer.join();
  compactor.join();
  EXPECT_FALSE(failed.load());
  Expected(2400, 1600, 2400, empty);
  PhysicalOracle(2400);
  db_->ReleaseSnapshot(empty);
  GC();
  Expected(800, 800, 2400);
  PhysicalOracle(2400);
  Reopen();
  Expected(800, 800, 2400);
}

}  // namespace
}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
