#include <atomic>
#include <cstdint>
#include <memory>
#include <set>
#include <string>

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/snapshot.h"
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
  Status NewWritableFile(const std::string& path, WritableFile** result) override {
    if (fail_cache_write && path == cache_path + ".tmp") {
      *result = nullptr;
      return Status::IOError("injected optional cache write failure");
    }
    return target()->NewWritableFile(path, result);
  }
  Status NewRandomAccessFile(const std::string& path,
                            RandomAccessFile** result) override {
    uint64_t number;
    FileType type;
    if (ParseFileName(path.substr(path.find_last_of('/') + 1), &number, &type) &&
        type == kTableFile) {
      ++tables_opened;
    }
    return target()->NewRandomAccessFile(path, result);
  }
  std::atomic<uint64_t> now{kEpoch};
  std::atomic<uint64_t> tables_opened{0};
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
  void ExpectFallback() {
    env_.tables_opened = 0;
    Open();
    EXPECT_FALSE(Impl()->TEST_StatsCacheLoaded());
    EXPECT_GT(env_.tables_opened.load(), uint64_t{0});
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

TEST_F(StatsCacheTest, MissingCacheRebuildsFromSSTs) {
  Prepare();
  Close();
  ASSERT_TRUE(env_.RemoveFile(env_.cache_path).ok());
  ExpectFallback();
}

TEST_F(StatsCacheTest, CorruptChecksumRebuildsFromSSTs) {
  Prepare();
  Close();
  std::string cache = ReadCache();
  ASSERT_GT(cache.size(), size_t{4});
  cache[cache.size() - 1] ^= 1;
  WriteCache(cache);
  ExpectFallback();
}

TEST_F(StatsCacheTest, TruncatedCacheAndIncompleteTemporaryFileAreIgnored) {
  Prepare();
  Close();
  WriteCache("truncated");
  ASSERT_TRUE(WriteStringToFile(&env_, "partial", env_.cache_path + ".tmp").ok());
  ExpectFallback();
  EXPECT_FALSE(env_.FileExists(env_.cache_path + ".tmp"));
}

TEST_F(StatsCacheTest, ChecksumValidMalformedCountsAreRejected) {
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
  ExpectFallback();
}

TEST_F(StatsCacheTest, ChecksumValidZeroReferenceIsRejected) {
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
  ExpectFallback();
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

TEST_F(StatsCacheTest, OptionalCacheWriteFailureDoesNotDamageDatabase) {
  Prepare();
  env_.fail_cache_write = true;
  Close();
  EXPECT_FALSE(env_.FileExists(env_.cache_path));
  env_.fail_cache_write = false;
  ExpectFallback();
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
