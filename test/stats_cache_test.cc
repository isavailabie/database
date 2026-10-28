#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <set>
#include <string>

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "db/version_edit.h"
#include "gtest/gtest.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace {

constexpr uint64_t kEpoch = 1700000000000000ULL;

class CacheEnv : public EnvWrapper {
 public:
  CacheEnv() : EnvWrapper(Env::Default()) {}
  uint64_t NowMicros() override { return now.load(); }

  class File : public WritableFile {
   public:
    File(WritableFile* file, CacheEnv* env, FileType type)
        : file_(file), env_(env), type_(type), descriptor_appends_(0) {}
    Status Append(const Slice& data) override {
      if (type_ == kDescriptorFile) {
        const int append = descriptor_appends_++;
        int expected = append;
        if (env_->fail_manifest_append_index.compare_exchange_strong(
                expected, -1)) {
          return Status::IOError("injected MANIFEST append failure");
        }
      }
      return file_->Append(data);
    }
    Status Close() override { return file_->Close(); }
    Status Flush() override { return file_->Flush(); }
    Status Sync() override {
      if (type_ == kDescriptorFile &&
          env_->fail_manifest_sync.exchange(false)) {
        return Status::IOError("injected MANIFEST sync failure");
      }
      return file_->Sync();
    }

   private:
    std::unique_ptr<WritableFile> file_;
    CacheEnv* env_;
    FileType type_;
    int descriptor_appends_;
  };

  Status NewWritableFile(const std::string& path, WritableFile** result) override {
    if (fail_cache_write && path == cache_path + ".tmp") {
      *result = nullptr;
      return Status::IOError("injected optional cache write failure");
    }
    uint64_t number = 0;
    FileType type = kTempFile;
    if (ParseFileName(path.substr(path.find_last_of('/') + 1), &number, &type) &&
        type == kDescriptorFile && fail_manifest_create.exchange(false)) {
      *result = nullptr;
      return Status::IOError("injected MANIFEST creation failure");
    }
    WritableFile* file = nullptr;
    Status status = target()->NewWritableFile(path, &file);
    *result = status.ok() ? new File(file, this, type) : nullptr;
    return status;
  }
  Status NewRandomAccessFile(const std::string& path,
                            RandomAccessFile** result) override {
    uint64_t number = 0;
    FileType type = kTempFile;
    if (ParseFileName(path.substr(path.find_last_of('/') + 1), &number, &type) &&
        type == kTableFile) {
      ++tables_opened;
    }
    return target()->NewRandomAccessFile(path, result);
  }
  Status GetFileSize(const std::string& path, uint64_t* size) override {
    Status status = target()->GetFileSize(path, size);
    uint64_t number;
    FileType type;
    if (status.ok() && force_manifest_rollover &&
        ParseFileName(path.substr(path.find_last_of('/') + 1), &number, &type) &&
        type == kDescriptorFile) {
      *size = 10 << 20;
    }
    return status;
  }
  Status RenameFile(const std::string& source,
                    const std::string& target_path) override {
    if (target_path == current_path && fail_current_rename.exchange(false)) {
      return Status::IOError("injected CURRENT rename failure");
    }
    return target()->RenameFile(source, target_path);
  }
  std::atomic<uint64_t> now{kEpoch};
  std::atomic<uint64_t> tables_opened{0};
  bool force_manifest_rollover = false;
  std::atomic<bool> fail_current_rename{false};
  std::atomic<bool> fail_manifest_create{false};
  std::atomic<bool> fail_manifest_sync{false};
  std::atomic<int> fail_manifest_append_index{-1};
  std::string current_path;
  bool fail_cache_write = false;
  std::string cache_path;
};

class StatsCacheTest : public testing::Test {
 protected:
  void SetUp() override {
    std::string directory;
    ASSERT_TRUE(env_.GetTestDirectory(&directory).ok());
    const auto* info = testing::UnitTest::GetInstance()->current_test_info();
    path_ = directory + "/stats-cache-" + info->name() + "-" +
            std::to_string(Env::Default()->NowMicros());
    options_.env = &env_;
    options_.create_if_missing = true;
    options_.compression = kNoCompression;
    env_.cache_path = path_ + "/STATS";
    env_.current_path = CurrentFileName(path_);
    ASSERT_TRUE(DestroyDB(path_, options_).ok());
    Open();
    ASSERT_NE(db_, nullptr);
  }
  void TearDown() override {
    db_.reset();
    EXPECT_TRUE(DestroyDB(path_, options_).ok());
    EXPECT_FALSE(env_.FileExists(path_));
  }
  void Open() {
    DB* raw = nullptr;
    const Status status = DB::Open(options_, path_, &raw);
    ASSERT_TRUE(status.ok()) << status.ToString();
    db_.reset(raw);
  }
  DBImpl* Impl() { return static_cast<DBImpl*>(db_.get()); }
  void Close() { db_.reset(); }
  void Flush() { ASSERT_TRUE(Impl()->TEST_CompactMemTable().ok()); }
  void Prepare() {
    ASSERT_TRUE(db_->Put(WriteOptions(), "ttl", "temporary", 1).ok());
    ASSERT_TRUE(db_->Put(WriteOptions(), "live", "permanent").ok());
    Flush();
  }
  void Expected(uint64_t entries, uint64_t distinct, uint64_t sequence) {
    Stats stats;
    ASSERT_TRUE(db_->GetCurrentStats(ReadOptions(), &stats).ok());
    EXPECT_EQ(entries, stats.total_entries);
    EXPECT_EQ(distinct, stats.distinct_keys);
    EXPECT_EQ(sequence, stats.seq);
    std::unique_ptr<Iterator> it(Impl()->TEST_NewInternalIterator());
    uint64_t total = 0;
    std::set<std::string> keys;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ParsedInternalKey key;
      ASSERT_TRUE(ParseInternalKey(it->key(), &key));
      keys.insert(key.user_key.ToString());
      ++total;
    }
    EXPECT_TRUE(it->status().ok());
    EXPECT_EQ(entries, total);
    EXPECT_EQ(distinct, keys.size());
    const Snapshot* snapshot = db_->GetSnapshot();
    EXPECT_EQ(sequence,
              static_cast<const SnapshotImpl*>(snapshot)->sequence_number());
    db_->ReleaseSnapshot(snapshot);
  }
  void VerifyRolloverFailure(int append_index, bool fail_create,
                             bool fail_sync) {
    Prepare();
    std::string current;
    ASSERT_TRUE(ReadFileToString(&env_, CurrentFileName(path_), &current).ok());
    ASSERT_FALSE(current.empty());
    ASSERT_EQ('\n', current.back());
    current.pop_back();
    const std::string previous = current;

    env_.force_manifest_rollover = true;
    env_.fail_manifest_create = fail_create;
    env_.fail_manifest_append_index = append_index;
    env_.fail_manifest_sync = fail_sync;
    ASSERT_TRUE(db_->Put(WriteOptions(), "rollover", "value").ok());
    const Status flush = Impl()->TEST_CompactMemTable();
    ASSERT_TRUE(flush.IsIOError()) << flush.ToString();

    ASSERT_TRUE(ReadFileToString(&env_, CurrentFileName(path_), &current).ok());
    ASSERT_FALSE(current.empty());
    ASSERT_EQ('\n', current.back());
    current.pop_back();
    EXPECT_EQ(previous, current);
    EXPECT_TRUE(env_.FileExists(path_ + "/" + previous));

    env_.force_manifest_rollover = false;
    env_.fail_manifest_create = false;
    env_.fail_manifest_append_index = -1;
    env_.fail_manifest_sync = false;
    Close();
    options_.reuse_logs = true;
    Open();
    Expected(3, 3, 3);
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), "rollover", &value).ok());
    EXPECT_EQ("value", value);
  }
  std::string ReadCache() {
    std::string contents;
    EXPECT_TRUE(ReadFileToString(&env_, env_.cache_path, &contents).ok());
    return contents;
  }
  void WriteCache(const std::string& contents) {
    ASSERT_TRUE(WriteStringToFile(&env_, contents, env_.cache_path).ok());
  }
  void RefreshCRC(std::string* contents) {
    ASSERT_GE(contents->size(), size_t{4});
    contents->resize(contents->size() - 4);
    PutFixed32(contents, crc32c::Value(contents->data(), contents->size()));
  }
  void ExpectManifestRecovery() {
    env_.tables_opened = 0;
    Open();
    EXPECT_FALSE(Impl()->TEST_StatsCacheLoaded());
    EXPECT_EQ(uint64_t{0}, env_.tables_opened.load());
    Expected(2, 2, 2);
  }
  CacheEnv env_;
  Options options_;
  std::string path_;
  std::unique_ptr<DB> db_;
};

TEST_F(StatsCacheTest, ValidCacheSkipsSSTDataReadsAcrossRepeatedOpens) {
  Prepare();
  for (bool reuse : {false, true, false, true}) {
    Close();
    ASSERT_TRUE(env_.FileExists(env_.cache_path));
    options_.reuse_logs = reuse;
    env_.tables_opened = 0;
    Open();
    EXPECT_TRUE(Impl()->TEST_StatsCacheLoaded());
    EXPECT_EQ(uint64_t{0}, env_.tables_opened.load());
    EXPECT_FALSE(env_.FileExists(env_.cache_path));
    Expected(2, 2, 2);
  }
}

TEST_F(StatsCacheTest, ExpiryDoesNotInvalidatePhysicalCounts) {
  Prepare();
  Close();
  env_.now += 1000000;
  Open();
  EXPECT_TRUE(Impl()->TEST_StatsCacheLoaded());
  Expected(2, 2, 2);
  std::string value;
  EXPECT_TRUE(db_->Get(ReadOptions(), "ttl", &value).IsNotFound());
  ASSERT_TRUE(db_->Get(ReadOptions(), "live", &value).ok());
  EXPECT_EQ("permanent", value);
  db_->CompactRange(nullptr, nullptr);
  Expected(1, 1, 2);
  Close();
  Open();
  EXPECT_TRUE(Impl()->TEST_StatsCacheLoaded());
  Expected(1, 1, 2);
}

TEST_F(StatsCacheTest, MissingCacheUsesManifestStats) {
  Prepare();
  Close();
  ASSERT_TRUE(env_.RemoveFile(env_.cache_path).ok());
  ExpectManifestRecovery();
}

TEST_F(StatsCacheTest, CorruptCacheUsesManifestStats) {
  Prepare();
  Close();
  std::string cache = ReadCache();
  ASSERT_GT(cache.size(), size_t{4});
  cache[cache.size() - 1] ^= 1;
  WriteCache(cache);
  ExpectManifestRecovery();
}

TEST_F(StatsCacheTest, TruncatedCacheAndIncompleteTemporaryFileAreIgnored) {
  Prepare();
  Close();
  WriteCache("truncated");
  ASSERT_TRUE(WriteStringToFile(&env_, "partial", env_.cache_path + ".tmp").ok());
  ExpectManifestRecovery();
  EXPECT_FALSE(env_.FileExists(env_.cache_path + ".tmp"));
}

TEST_F(StatsCacheTest, ChecksumValidMalformedCacheUsesManifestStats) {
  Prepare();
  Close();
  std::string cache = ReadCache();
  Slice payload(cache.data(), cache.size() - 4);
  Slice magic, fingerprint;
  ASSERT_TRUE(GetLengthPrefixedSlice(&payload, &magic));
  ASSERT_TRUE(GetLengthPrefixedSlice(&payload, &fingerprint));
  const size_t offset = payload.data() - cache.data();
  EncodeFixed64(&cache[offset], 999);
  RefreshCRC(&cache);
  WriteCache(cache);
  ExpectManifestRecovery();
}

TEST_F(StatsCacheTest, ChecksumValidZeroReferenceCacheUsesManifestStats) {
  Prepare();
  Close();
  std::string cache = ReadCache();
  Slice payload(cache.data(), cache.size() - 4);
  Slice ignored, key;
  ASSERT_TRUE(GetLengthPrefixedSlice(&payload, &ignored));
  ASSERT_TRUE(GetLengthPrefixedSlice(&payload, &ignored));
  payload.remove_prefix(16);
  ASSERT_TRUE(GetLengthPrefixedSlice(&payload, &key));
  const size_t offset = payload.data() - cache.data();
  EncodeFixed64(&cache[offset], 0);
  RefreshCRC(&cache);
  WriteCache(cache);
  ExpectManifestRecovery();
}

TEST_F(StatsCacheTest, StaleSequenceCacheCannotHideNewWrites) {
  Prepare();
  Close();
  const std::string old_cache = ReadCache();
  Open();
  ASSERT_TRUE(Impl()->TEST_StatsCacheLoaded());
  ASSERT_TRUE(db_->Put(WriteOptions(), "new", "x").ok());
  Flush();
  Close();
  WriteCache(old_cache);
  Open();
  EXPECT_FALSE(Impl()->TEST_StatsCacheLoaded());
  Expected(3, 3, 3);
}

TEST_F(StatsCacheTest, SameSequenceAfterCompactionRejectsOldSSTFingerprint) {
  Prepare();
  Close();
  const std::string old_cache = ReadCache();
  Open();
  env_.now += 1000000;
  db_->CompactRange(nullptr, nullptr);
  Expected(1, 1, 2);
  Close();
  WriteCache(old_cache);
  Open();
  EXPECT_FALSE(Impl()->TEST_StatsCacheLoaded());
  Expected(1, 1, 2);
}

TEST_F(StatsCacheTest, WALBackedRecordsDoNotUseStaleCacheInEitherReuseMode) {
  Prepare();
  Close();
  const std::string stale = ReadCache();
  Open();
  WriteOptions synced;
  synced.sync = true;
  ASSERT_TRUE(db_->Put(synced, "wal", "x").ok());
  Close();
  EXPECT_FALSE(env_.FileExists(env_.cache_path));
  for (bool reuse : {true, false}) {
    options_.reuse_logs = reuse;
    WriteCache(stale);
    Open();
    EXPECT_FALSE(Impl()->TEST_StatsCacheLoaded());
    Expected(3, 3, 3);
    Close();
  }
  Open();
}

TEST_F(StatsCacheTest, ManifestStatsTrackFlushCompactionAndWALRecovery) {
  Prepare();
  Close();
  ASSERT_TRUE(env_.RemoveFile(env_.cache_path).ok());
  env_.tables_opened = 0;
  Open();
  EXPECT_FALSE(Impl()->TEST_StatsCacheLoaded());
  EXPECT_EQ(uint64_t{0}, env_.tables_opened.load());
  Expected(2, 2, 2);

  ASSERT_TRUE(db_->Put(WriteOptions(), "live", "new").ok());
  Flush();
  Expected(3, 2, 3);
  env_.now += 1000000;
  db_->CompactRange(nullptr, nullptr);
  Expected(1, 1, 3);

  Close();
  ASSERT_TRUE(env_.RemoveFile(env_.cache_path).ok());
  env_.tables_opened = 0;
  Open();
  EXPECT_FALSE(Impl()->TEST_StatsCacheLoaded());
  EXPECT_EQ(uint64_t{0}, env_.tables_opened.load());
  Expected(1, 1, 3);

  ASSERT_TRUE(db_->Put(WriteOptions(), "wal", "value").ok());
  Close();
  options_.reuse_logs = true;
  env_.tables_opened = 0;
  Open();
  EXPECT_FALSE(Impl()->TEST_StatsCacheLoaded());
  EXPECT_EQ(uint64_t{0}, env_.tables_opened.load());
  Expected(2, 2, 4);
}

TEST_F(StatsCacheTest, ManifestStatsDeltaUnderflowFallsBackToSSTScan) {
  Prepare();
  Close();
  ASSERT_TRUE(env_.RemoveFile(env_.cache_path).ok());

  std::string current;
  ASSERT_TRUE(ReadFileToString(&env_, CurrentFileName(path_), &current).ok());
  ASSERT_FALSE(current.empty());
  ASSERT_EQ('\n', current.back());
  current.pop_back();
  const std::string manifest = path_ + "/" + current;
  uint64_t manifest_size = 0;
  ASSERT_TRUE(env_.GetFileSize(manifest, &manifest_size).ok());
  WritableFile* file = nullptr;
  ASSERT_TRUE(env_.NewAppendableFile(manifest, &file).ok());
  {
    log::Writer writer(file, manifest_size);
    VersionEdit bad_delta;
    bad_delta.AddStatsDelta("live", 3, false);
    std::string record;
    bad_delta.EncodeTo(&record);
    ASSERT_TRUE(writer.AddRecord(record).ok());
  }
  ASSERT_TRUE(file->Sync().ok());
  ASSERT_TRUE(file->Close().ok());
  delete file;

  options_.create_if_missing = false;
  env_.tables_opened = 0;
  Open();
  EXPECT_FALSE(Impl()->TEST_StatsCacheLoaded());
  EXPECT_GT(env_.tables_opened.load(), uint64_t{0});
  Expected(2, 2, 2);
}

TEST_F(StatsCacheTest, ManifestStatsDeltaOverflowFallsBackToSSTScan) {
  Prepare();
  Close();
  ASSERT_TRUE(env_.RemoveFile(env_.cache_path).ok());

  std::string current;
  ASSERT_TRUE(ReadFileToString(&env_, CurrentFileName(path_), &current).ok());
  ASSERT_FALSE(current.empty());
  ASSERT_EQ('\n', current.back());
  current.pop_back();
  const std::string manifest = path_ + "/" + current;
  uint64_t manifest_size = 0;
  ASSERT_TRUE(env_.GetFileSize(manifest, &manifest_size).ok());
  WritableFile* file = nullptr;
  ASSERT_TRUE(env_.NewAppendableFile(manifest, &file).ok());
  {
    log::Writer writer(file, manifest_size);
    VersionEdit bad_delta;
    bad_delta.AddStatsDelta("live", std::numeric_limits<uint64_t>::max(),
                            true);
    std::string record;
    bad_delta.EncodeTo(&record);
    ASSERT_TRUE(writer.AddRecord(record).ok());
  }
  ASSERT_TRUE(file->Sync().ok());
  ASSERT_TRUE(file->Close().ok());
  delete file;

  options_.create_if_missing = false;
  env_.tables_opened = 0;
  Open();
  EXPECT_FALSE(Impl()->TEST_StatsCacheLoaded());
  EXPECT_GT(env_.tables_opened.load(), uint64_t{0});
  Expected(2, 2, 2);
}

TEST_F(StatsCacheTest, ManifestRolloverKeepsStatsSnapshotAndReclaimsOldLog) {
  Prepare();
  std::string current;
  ASSERT_TRUE(ReadFileToString(&env_, CurrentFileName(path_), &current).ok());
  ASSERT_FALSE(current.empty());
  ASSERT_EQ('\n', current.back());
  current.pop_back();
  const std::string previous = current;
  env_.force_manifest_rollover = true;
  ASSERT_TRUE(db_->Put(WriteOptions(), "rollover", "value").ok());
  Flush();
  ASSERT_TRUE(ReadFileToString(&env_, CurrentFileName(path_), &current).ok());
  ASSERT_FALSE(current.empty());
  ASSERT_EQ('\n', current.back());
  current.pop_back();
  EXPECT_NE(previous, current);
  EXPECT_FALSE(env_.FileExists(path_ + "/" + previous));
  Expected(3, 3, 3);

  Close();
  env_.force_manifest_rollover = false;
  ASSERT_TRUE(env_.RemoveFile(env_.cache_path).ok());
  env_.tables_opened = 0;
  Open();
  EXPECT_FALSE(Impl()->TEST_StatsCacheLoaded());
  EXPECT_EQ(uint64_t{0}, env_.tables_opened.load());
  Expected(3, 3, 3);
}

TEST_F(StatsCacheTest, FailedManifestRolloverKeepsOldRecoveryPoint) {
  Prepare();
  std::string current;
  ASSERT_TRUE(ReadFileToString(&env_, CurrentFileName(path_), &current).ok());
  ASSERT_FALSE(current.empty());
  ASSERT_EQ('\n', current.back());
  current.pop_back();
  const std::string previous = current;

  env_.force_manifest_rollover = true;
  env_.fail_current_rename = true;
  ASSERT_TRUE(db_->Put(WriteOptions(), "rollover", "value").ok());
  const Status flush = Impl()->TEST_CompactMemTable();
  EXPECT_TRUE(flush.IsIOError()) << flush.ToString();
  ASSERT_TRUE(ReadFileToString(&env_, CurrentFileName(path_), &current).ok());
  ASSERT_FALSE(current.empty());
  ASSERT_EQ('\n', current.back());
  current.pop_back();
  EXPECT_EQ(previous, current);
  EXPECT_TRUE(env_.FileExists(path_ + "/" + previous));

  env_.force_manifest_rollover = false;
  env_.fail_current_rename = false;
  Close();
  options_.reuse_logs = true;
  Open();
  Expected(3, 3, 3);
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "rollover", &value).ok());
  EXPECT_EQ("value", value);
}

TEST_F(StatsCacheTest, ManifestRolloverCreationFailureKeepsOldRecoveryPoint) {
  VerifyRolloverFailure(-1, true, false);
}

TEST_F(StatsCacheTest, ManifestRolloverEditHeaderFailureKeepsOldRecoveryPoint) {
  VerifyRolloverFailure(2, false, false);
}

TEST_F(StatsCacheTest, ManifestRolloverEditPayloadFailureKeepsOldRecoveryPoint) {
  VerifyRolloverFailure(3, false, false);
}

TEST_F(StatsCacheTest, ManifestRolloverSyncFailureKeepsOldRecoveryPoint) {
  VerifyRolloverFailure(-1, false, true);
}

TEST_F(StatsCacheTest, OptionalCacheWriteFailureDoesNotDamageDatabase) {
  Prepare();
  env_.fail_cache_write = true;
  Close();
  EXPECT_FALSE(env_.FileExists(env_.cache_path));
  env_.fail_cache_write = false;
  ExpectManifestRecovery();
  Close();
  Open();
  EXPECT_TRUE(Impl()->TEST_StatsCacheLoaded());
  Expected(2, 2, 2);
}

TEST_F(StatsCacheTest, DestroyDBRemovesCacheAndAllowsFreshEmptyDatabase) {
  Prepare();
  Close();
  ASSERT_TRUE(env_.FileExists(env_.cache_path));
  ASSERT_TRUE(WriteStringToFile(&env_, "partial", env_.cache_path + ".tmp").ok());
  ASSERT_TRUE(DestroyDB(path_, options_).ok());
  EXPECT_FALSE(env_.FileExists(path_));
  Open();
  EXPECT_FALSE(Impl()->TEST_StatsCacheLoaded());
  Expected(0, 0, 0);
}

}  // namespace
}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
