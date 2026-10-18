#include <atomic>
#include <cstdint>
#include <memory>
#include <set>
#include <string>

#if defined(__unix__) || defined(__APPLE__)
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "gtest/gtest.h"
#include "leveldb/db.h"
#include "leveldb/env.h"

namespace leveldb {
namespace {

constexpr uint64_t kEpoch = 1700000000000000ULL;
std::string executable;

class CommitEnv : public EnvWrapper {
 public:
  CommitEnv() : EnvWrapper(Env::Default()) {}
  uint64_t NowMicros() override { return now; }
  uint64_t now = kEpoch;
  std::atomic<int> crash_point{0};

  class File : public WritableFile {
   public:
    File(WritableFile* file, CommitEnv* env, FileType type)
        : file_(file), env_(env), type_(type) {}
    Status Append(const Slice& data) override { return file_->Append(data); }
    Status Close() override { return file_->Close(); }
    Status Flush() override { return file_->Flush(); }
    Status Sync() override {
      const Status status = file_->Sync();
#if defined(__unix__) || defined(__APPLE__)
      if (status.ok()) {
        const int point = env_->crash_point.load();
        if ((point == 1 && type_ == kTableFile) ||
            (point == 2 && type_ == kDescriptorFile)) {
          // Exit only after the underlying Sync succeeded, without DB cleanup.
          _exit(70 + point);
        }
      }
#endif
      return status;
    }
   private:
    std::unique_ptr<WritableFile> file_;
    CommitEnv* env_;
    FileType type_;
  };

  Status NewWritableFile(const std::string& path, WritableFile** result) override {
    WritableFile* file = nullptr;
    const Status status = target()->NewWritableFile(path, &file);
    Wrap(path, file, status, result);
    return status;
  }
  Status NewAppendableFile(const std::string& path, WritableFile** result) override {
    WritableFile* file = nullptr;
    const Status status = target()->NewAppendableFile(path, &file);
    Wrap(path, file, status, result);
    return status;
  }
 private:
  void Wrap(const std::string& path, WritableFile* file, const Status& status,
            WritableFile** result) {
    uint64_t number;
    FileType type = kTempFile;
    ParseFileName(path.substr(path.find_last_of('/') + 1), &number, &type);
    *result = status.ok() ? new File(file, this, type) : nullptr;
  }
};

class CompactionRecoveryTest : public testing::Test {
 protected:
  void SetUp() override {
    std::string directory;
    ASSERT_TRUE(env_.GetTestDirectory(&directory).ok());
    const auto* info = testing::UnitTest::GetInstance()->current_test_info();
    path_ = directory + "/commit-recovery-" + info->name() + "-" +
            std::to_string(Env::Default()->NowMicros());
    options_.env = &env_;
    options_.create_if_missing = true;
    options_.compression = kNoCompression;
    ASSERT_TRUE(DestroyDB(path_, options_).ok());
    Open();
    ASSERT_NE(db_, nullptr);
  }
  void TearDown() override {
    db_.reset();
    EXPECT_TRUE(DestroyDB(path_, options_).ok());
  }
  void Open() {
    DB* raw = nullptr;
    const Status status = DB::Open(options_, path_, &raw);
    ASSERT_TRUE(status.ok()) << status.ToString();
    db_.reset(raw);
  }
  void CheckStats(uint64_t entries, uint64_t distinct) {
    Stats stats;
    ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
    EXPECT_EQ(entries, stats.total_entries);
    EXPECT_EQ(distinct, stats.distinct_keys);
    EXPECT_EQ(uint64_t{3}, stats.seq);
    std::unique_ptr<Iterator> it(
        static_cast<DBImpl*>(db_.get())->TEST_NewInternalIterator());
    uint64_t count = 0;
    std::set<std::string> keys;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ParsedInternalKey key;
      ASSERT_TRUE(ParseInternalKey(it->key(), &key));
      keys.insert(key.user_key.ToString());
      ++count;
    }
    ASSERT_TRUE(it->status().ok());
    EXPECT_EQ(entries, count);
    EXPECT_EQ(distinct, keys.size());
  }
  void CheckReads() {
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), "live", &value).ok());
    EXPECT_EQ("permanent", value);
    EXPECT_TRUE(db_->Get(ReadOptions(), "a", &value).IsNotFound());
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ("live", it->key().ToString());
    it->Next();
    EXPECT_FALSE(it->Valid());
    EXPECT_TRUE(it->status().ok());
  }
  void Exercise(int point) {
#if defined(__unix__) || defined(__APPLE__)
    WriteOptions write;
    write.sync = true;
    ASSERT_TRUE(db_->Put(write, "a", "old").ok());
    ASSERT_TRUE(db_->Put(write, "a", "temporary", 1).ok());
    ASSERT_TRUE(db_->Put(write, "live", "permanent").ok());
    auto* impl = static_cast<DBImpl*>(db_.get());
    ASSERT_TRUE(impl->TEST_CompactMemTable().ok());
    for (int level = 0; level < config::kNumLevels - 1; ++level) {
      impl->TEST_CompactRange(level, nullptr, nullptr);
    }
    // Compaction retained the live TTL version but collected its normal old value.
    CheckStats(2, 2);
    db_.reset();
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      const std::string argument = std::to_string(point);
      execl(executable.c_str(), executable.c_str(), "--crash-compact",
            path_.c_str(), argument.c_str(), static_cast<char*>(nullptr));
      _exit(127);
    }
    int status = 0;
    ASSERT_EQ(child, waitpid(child, &status, 0));
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(70 + point, WEXITSTATUS(status));
    env_.now = kEpoch + 2000000;
    options_.create_if_missing = false;
    Open();
    CheckReads();
    // SST-only commit leaves old inputs current; durable MANIFEST commits the GC.
    CheckStats(point == 1 ? 2 : 1, point == 1 ? 2 : 1);
    db_->CompactRange(nullptr, nullptr);
    CheckReads();
    CheckStats(1, 1);
    db_.reset();
    Open();
    CheckReads();
    CheckStats(1, 1);
#else
    GTEST_SKIP() << "process crash injection requires POSIX";
#endif
  }
  CommitEnv env_;
  Options options_;
  std::string path_;
  std::unique_ptr<DB> db_;
};

TEST_F(CompactionRecoveryTest, CrashAfterSSTSyncBeforeManifestCommit) {
  Exercise(1);
}
TEST_F(CompactionRecoveryTest, CrashAfterManifestSyncBeforeOldFilesCleanup) {
  Exercise(2);
}

int CrashCompact(const std::string& path, int point) {
  CommitEnv env;
  env.now = kEpoch + 2000000;
  Options options;
  options.env = &env;
  options.create_if_missing = false;
  options.reuse_logs = true;
  DB* raw = nullptr;
  if (!DB::Open(options, path, &raw).ok()) return 10;
  std::unique_ptr<DB> db(raw);
  env.crash_point = point;
  // Target the prepared L6 directly: public CompactRange first commits a
  // memtable/log rotation, whose MANIFEST Sync is not the GC commit under test.
  static_cast<DBImpl*>(db.get())->TEST_CompactRange(config::kNumLevels - 1,
                                                   nullptr, nullptr);
  return 11;
}

}  // namespace
}  // namespace leveldb

int main(int argc, char** argv) {
  if (argc == 4 && std::string(argv[1]) == "--crash-compact") {
    return leveldb::CrashCompact(argv[2], std::stoi(argv[3]));
  }
  leveldb::executable = argv[0];
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
