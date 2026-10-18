#include <atomic>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/snapshot.h"
#include "db/write_batch_internal.h"
#include "gtest/gtest.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"

namespace leveldb {
namespace {

constexpr uint64_t kSecond = 1000000;
constexpr uint64_t kStart = 1700000000000000ULL;

class ClockEnv : public EnvWrapper {
 public:
  ClockEnv() : EnvWrapper(Env::Default()) {}
  uint64_t NowMicros() override { return now.load(); }
  std::atomic<uint64_t> now{kStart};
};

class TTLReadViewTest : public testing::Test {
 protected:
  void SetUp() override {
    std::string dir;
    ASSERT_TRUE(env_.GetTestDirectory(&dir).ok());
    static std::atomic<uint64_t> next{0};
    const auto* info = testing::UnitTest::GetInstance()->current_test_info();
    name_ = dir + "/ttl-view-" + info->name() + "-" +
            std::to_string(Env::Default()->NowMicros()) + "-" +
            std::to_string(next++);
    options_.env = &env_;
    options_.create_if_missing = true;
    options_.compression = kNoCompression;
    ASSERT_TRUE(DestroyDB(name_, options_).ok());
    Open();
    ASSERT_NE(db_, nullptr);
  }

  void TearDown() override {
    db_.reset();
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
    options_.create_if_missing = false;
    Open();
  }

  void Flush() {
    ASSERT_TRUE(static_cast<DBImpl*>(db_.get())->TEST_CompactMemTable().ok());
  }

  uint64_t Sequence() {
    const Snapshot* view = db_->GetSnapshot();
    const uint64_t seq = static_cast<const SnapshotImpl*>(view)->sequence_number();
    db_->ReleaseSnapshot(view);
    return seq;
  }

  void Value(const std::string& key, const std::string& expected,
             const Snapshot* view = nullptr) {
    ReadOptions read;
    read.snapshot = view;
    std::string value;
    const Status s = db_->Get(read, key, &value);
    ASSERT_TRUE(s.ok()) << key << ": " << s.ToString();
    EXPECT_EQ(expected, value);
  }

  void Missing(const std::string& key, const Snapshot* view = nullptr) {
    ReadOptions read;
    read.snapshot = view;
    std::string value = "unchanged";
    const Status s = db_->Get(read, key, &value);
    EXPECT_TRUE(s.IsNotFound()) << key << ": " << s.ToString();
    EXPECT_EQ("unchanged", value);
  }

  void CheckIterator(Iterator* it,
                     const std::map<std::string, std::string>& expected) {
    auto forward = expected.begin();
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ASSERT_NE(forward, expected.end());
      EXPECT_EQ(forward->first, it->key().ToString());
      EXPECT_EQ(forward->second, it->value().ToString());
      ++forward;
    }
    EXPECT_EQ(forward, expected.end());
    EXPECT_TRUE(it->status().ok()) << it->status().ToString();
    auto reverse = expected.rbegin();
    for (it->SeekToLast(); it->Valid(); it->Prev()) {
      ASSERT_NE(reverse, expected.rend());
      EXPECT_EQ(reverse->first, it->key().ToString());
      EXPECT_EQ(reverse->second, it->value().ToString());
      ++reverse;
    }
    EXPECT_EQ(reverse, expected.rend());
    EXPECT_TRUE(it->status().ok()) << it->status().ToString();
    for (const auto& item : expected) {
      it->Seek(item.first);
      ASSERT_TRUE(it->Valid());
      EXPECT_EQ(item.first, it->key().ToString());
      EXPECT_EQ(item.second, it->value().ToString());
    }
  }

  void View(const std::map<std::string, std::string>& expected,
            const Snapshot* view = nullptr) {
    ReadOptions read;
    read.snapshot = view;
    std::unique_ptr<Iterator> it(db_->NewIterator(read));
    CheckIterator(it.get(), expected);
  }

  ClockEnv env_;
  Options options_;
  std::string name_;
  std::unique_ptr<DB> db_;
};

TEST_F(TTLReadViewTest, ExactDeadlineAndOverflowSequence) {
  ASSERT_EQ(0, Sequence());
  ASSERT_TRUE(db_->Put(WriteOptions(), "plain", "plain").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "ttl", "live", 1).ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "zero", "expired", 0).ok());
  EXPECT_EQ(3, Sequence());
  Missing("zero");
  env_.now = kStart + kSecond - 1;
  Value("ttl", "live");
  env_.now = kStart + kSecond;
  Missing("ttl");
  Value("plain", "plain");
  ASSERT_TRUE(db_->Put(WriteOptions(), "ttl", "overflow",
                       std::numeric_limits<uint64_t>::max()).IsInvalidArgument());
  env_.now = std::numeric_limits<uint64_t>::max() - 10;
  ASSERT_TRUE(db_->Put(WriteOptions(), "new", "overflow", 1).IsInvalidArgument());
  EXPECT_EQ(3, Sequence());
  Missing("new");
  Missing("ttl");
  ASSERT_TRUE(db_->Put(WriteOptions(), "after", "plain").ok());
  EXPECT_EQ(4, Sequence());
}

TEST_F(TTLReadViewTest, ZeroEpochAndMaximumDeadline) {
  env_.now = 0;
  ASSERT_TRUE(db_->Put(WriteOptions(), "zero", "x", 0).ok());
  Missing("zero");
  env_.now = std::numeric_limits<uint64_t>::max() - kSecond;
  ASSERT_TRUE(db_->Put(WriteOptions(), "maximum", "x", 1).ok());
  Value("maximum", "x");
  env_.now = std::numeric_limits<uint64_t>::max();
  Missing("maximum");
  EXPECT_EQ(2, Sequence());
}

TEST_F(TTLReadViewTest, BinaryAndEmptyValuesRemainUnchanged) {
  const std::string key("k\0ey", 4);
  const std::string binary("\0\0\0\0\0\0\0\0\xffpayload", 16);
  ASSERT_TRUE(db_->Put(WriteOptions(), key, binary).ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "ttl", binary, 2).ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "", "", 2).ok());
  Value(key, binary);
  Value("ttl", binary);
  Value("", "");
  View({{"", ""}, {key, binary}, {"ttl", binary}});
  Flush();
  Reopen();
  Value(key, binary);
  Value("ttl", binary);
  env_.now += 2 * kSecond;
  View({{key, binary}});
}

TEST_F(TTLReadViewTest, ExpiredOverwriteNeverFallsBackAcrossSources) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "old").ok());
  Flush();
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "temporary", 1).ok());
  env_.now += kSecond;
  Missing("a");
  View({});
  Flush();
  Missing("a");
  db_->CompactRange(nullptr, nullptr);
  Missing("a");
  Reopen();
  Missing("a");
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "revived").ok());
  Value("a", "revived");
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "zero", 0).ok());
  Missing("a");
  WriteBatch batch;
  batch.Put("a", "permanent");
  batch.Put("b", "discard");
  batch.Delete("b");
  batch.Delete("absent");
  ASSERT_TRUE(db_->Write(WriteOptions(), &batch).ok());
  View({{"a", "permanent"}});
  EXPECT_EQ(8, Sequence());
  ASSERT_TRUE(db_->Delete(WriteOptions(), "a").ok());
  Missing("a");
}

TEST_F(TTLReadViewTest, SnapshotAndLazyIteratorFreezeTimeAndSequence) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "a", "ttl", 1).ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "b", "old").ok());
  Flush();
  const Snapshot* before = db_->GetSnapshot();
  std::unique_ptr<Iterator> pinned(db_->NewIterator(ReadOptions()));
  env_.now += kSecond;
  const Snapshot* after = db_->GetSnapshot();
  EXPECT_EQ(static_cast<const SnapshotImpl*>(before)->sequence_number(),
            static_cast<const SnapshotImpl*>(after)->sequence_number());
  ASSERT_TRUE(db_->Put(WriteOptions(), "b", "new").ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "c", "new").ok());
  Missing("a");
  Value("a", "ttl", before);
  Missing("a", after);
  View({{"a", "ttl"}, {"b", "old"}}, before);
  View({{"b", "old"}}, after);
  db_->CompactRange(nullptr, nullptr);
  CheckIterator(pinned.get(), {{"a", "ttl"}, {"b", "old"}});
  Value("a", "ttl", before);
  db_->ReleaseSnapshot(before);
  db_->ReleaseSnapshot(after);
  db_->CompactRange(nullptr, nullptr);
  CheckIterator(pinned.get(), {{"a", "ttl"}, {"b", "old"}});
  View({{"b", "new"}, {"c", "new"}});
}

TEST_F(TTLReadViewTest, SeekAndDirectionChangesSkipExpiredNewVersions) {
  for (const std::string key : {"a", "b", "c", "d", "e", "f", "g"}) {
    ASSERT_TRUE(db_->Put(WriteOptions(), key, "old").ok());
    if (key == "a" || key == "c" || key == "e" || key == "g") {
      ASSERT_TRUE(db_->Put(WriteOptions(), key, "expired", 1).ok());
    } else {
      ASSERT_TRUE(db_->Put(WriteOptions(), key, key, 10).ok());
    }
  }
  Flush();
  env_.now += kSecond;
  std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
  CheckIterator(it.get(), {{"b", "b"}, {"d", "d"}, {"f", "f"}});
  it->Seek("c");
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ("d", it->key().ToString());
  it->Next();
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ("f", it->key().ToString());
  it->Prev();
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ("d", it->key().ToString());
  it->Next();
  it->Prev();
  it->Prev();
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ("b", it->key().ToString());
  it->Prev();
  EXPECT_FALSE(it->Valid());
  it->Seek("g");
  EXPECT_FALSE(it->Valid());
  EXPECT_TRUE(it->status().ok());
}

TEST_F(TTLReadViewTest, RecoverWALWithAndWithoutLogReuse) {
  for (bool reuse : {false, true}) {
    options_.reuse_logs = reuse;
    Reopen();
    WriteOptions synced;
    synced.sync = true;
    const uint64_t start = env_.now.load();
    ASSERT_TRUE(db_->Put(synced, "ttl", "wal", 2).ok());
    ASSERT_TRUE(db_->Put(synced, "plain", "wal").ok());
    env_.now = start + kSecond;
    Reopen();
    Value("ttl", "wal");
    env_.now = start + 2 * kSecond;
    Missing("ttl");
    Value("plain", "wal");
    Reopen();
    Missing("ttl");
  }
  EXPECT_EQ(4, Sequence());
}

TEST_F(TTLReadViewTest, FlushingAndCompactionDoNotExtendTTL) {
  ASSERT_TRUE(db_->Put(WriteOptions(), "ttl", "sst", 3).ok());
  ASSERT_TRUE(db_->Put(WriteOptions(), "plain", "sst").ok());
  Flush();
  env_.now += kSecond;
  db_->CompactRange(nullptr, nullptr);
  Reopen();
  Value("ttl", "sst");
  env_.now = kStart + 3 * kSecond;
  Missing("ttl");
  Value("plain", "sst");
  EXPECT_EQ(2, Sequence());
  Reopen();
  Missing("ttl");
  EXPECT_EQ(2, Sequence());
}

TEST_F(TTLReadViewTest, RandomizedOperationsAgainstIndependentModel) {
  std::mt19937 rng(739);
  std::map<std::string, std::string> model;
  for (int i = 0; i < 250; ++i) {
    const std::string key = "key-" + std::to_string(rng() % 15);
    const std::string value = std::to_string(i);
    const unsigned operation = rng() % 4;
    if (operation == 0) {
      ASSERT_TRUE(db_->Delete(WriteOptions(), key).ok());
      model.erase(key);
    } else if (operation == 1) {
      ASSERT_TRUE(db_->Put(WriteOptions(), key, value, 0).ok());
      model.erase(key);
    } else if (operation == 2) {
      ASSERT_TRUE(db_->Put(WriteOptions(), key, value, 3600).ok());
      model[key] = value;
    } else {
      ASSERT_TRUE(db_->Put(WriteOptions(), key, value).ok());
      model[key] = value;
    }
    if (i % 25 == 0) {
      View(model);
      Flush();
    }
  }
  View(model);
  db_->CompactRange(nullptr, nullptr);
  Reopen();
  View(model);
  EXPECT_EQ(250, Sequence());
}

TEST_F(TTLReadViewTest, ConcurrentWritesReadsAndIterators) {
  std::atomic<bool> failed{false};
  std::vector<std::thread> writers;
  for (int thread = 0; thread < 4; ++thread) {
    writers.emplace_back([&, thread] {
      for (int i = 0; i < 200; ++i) {
        const std::string key = std::to_string(thread) + "/" + std::to_string(i);
        if (!db_->Put(WriteOptions(), key, key, 1).ok()) failed = true;
        std::string value;
        if (!db_->Get(ReadOptions(), key, &value).ok() || value != key) failed = true;
        if (i % 50 == 0) {
          std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
          for (it->SeekToFirst(); it->Valid(); it->Next()) {
            if (it->key().ToString() != it->value().ToString()) failed = true;
          }
          if (!it->status().ok()) failed = true;
        }
      }
    });
  }
  for (auto& writer : writers) writer.join();
  EXPECT_FALSE(failed.load());
  EXPECT_EQ(800, Sequence());
  env_.now += kSecond;
  View({});
  Reopen();
  View({});
  EXPECT_EQ(800, Sequence());
}

TEST_F(TTLReadViewTest, ManyOverwrittenTTLRecordsAcrossSSTs) {
  options_.write_buffer_size = 128 << 10;
  options_.max_file_size = 128 << 10;
  Reopen();
  std::mt19937 rng(0);
  const std::string payload(2048, 'x');
  for (int i = 0; i < 5000; ++i) {
    ASSERT_TRUE(db_->Put(WriteOptions(), std::to_string(rng() % 500),
                         payload, 20).ok());
  }
  Flush();
  for (int i = 0; i < 500; ++i) Value(std::to_string(i), payload);
  env_.now += 20 * kSecond;
  for (int i = 0; i < 500; ++i) Missing(std::to_string(i));
  View({});
  Reopen();
  for (int i = 0; i < 500; ++i) Missing(std::to_string(i));
}

TEST_F(TTLReadViewTest, DefaultEnvRealClockExpiry) {
  options_.env = Env::Default();
  options_.write_buffer_size = 128 << 10;
  Reopen();
  std::mt19937 rng(0);
  for (int i = 0; i < 5000; ++i) {
    ASSERT_TRUE(db_->Put(WriteOptions(), std::to_string(rng() % 500),
                         std::string(2048, 'x'), 1).ok());
  }
  Env::Default()->SleepForMicroseconds(1100000);
  for (int i = 0; i < 500; ++i) Missing(std::to_string(i));
  View({});
}

class LegacyBatchHandler : public WriteBatch::Handler {
 public:
  void Put(const Slice& key, const Slice& value) override {
    records.push_back(key.ToString() + "=" + value.ToString());
  }
  void Delete(const Slice& key) override {
    records.push_back("delete:" + key.ToString());
  }
  std::vector<std::string> records;
};

TEST(TTLBatchTest, AppendPreservesDeadlineTypeAndSequence) {
  WriteBatch batch, tail;
  batch.Put("a", "plain");
  WriteBatchInternal::PutWithExpiration(&batch, "b", "ttl", 1234);
  tail.Delete("c");
  WriteBatchInternal::PutWithExpiration(&tail, "d", "", 5678);
  batch.Append(tail);
  WriteBatchInternal::SetSequence(&batch, 10);
  ASSERT_EQ(4, WriteBatchInternal::Count(&batch));
  LegacyBatchHandler legacy;
  ASSERT_TRUE(batch.Iterate(&legacy).ok());
  EXPECT_EQ((std::vector<std::string>{"a=plain", "b=ttl", "delete:c", "d="}),
            legacy.records);
  auto* mem = new MemTable(InternalKeyComparator(BytewiseComparator()));
  mem->Ref();
  ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, mem).ok());
  {
    std::unique_ptr<Iterator> it(mem->NewIterator());
    uint64_t count = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next(), ++count) {
      ParsedInternalKey key;
      ASSERT_TRUE(ParseInternalKey(it->key(), &key));
      EXPECT_EQ(10 + count, key.sequence);
      if (key.user_key == Slice("b") || key.user_key == Slice("d")) {
        EXPECT_EQ(kTypeTTLValue, key.type);
        ASSERT_GE(it->value().size(), 8);
        EXPECT_EQ(key.user_key == Slice("b") ? 1234 : 5678,
                  DecodeFixed64(it->value().data()));
      }
    }
    EXPECT_EQ(4, count);
    EXPECT_TRUE(it->status().ok());
  }
  mem->Unref();
  batch.Clear();
  EXPECT_EQ(0, WriteBatchInternal::Count(&batch));
}

TEST(TTLBatchTest, RejectsTruncatedDeadlinesAndUnknownTags) {
  for (size_t size = 0; size < 8; ++size) {
    std::string raw(12, '\0');
    raw.push_back(static_cast<char>(kTypeTTLValue));
    PutLengthPrefixedSlice(&raw, "key");
    PutLengthPrefixedSlice(&raw, std::string(size, 'x'));
    WriteBatch batch;
    WriteBatchInternal::SetContents(&batch, raw);
    WriteBatchInternal::SetCount(&batch, 1);
    LegacyBatchHandler handler;
    EXPECT_TRUE(batch.Iterate(&handler).IsCorruption());
    EXPECT_TRUE(handler.records.empty());
  }
  std::string raw(12, '\0');
  raw.push_back(3);
  WriteBatch batch;
  WriteBatchInternal::SetContents(&batch, raw);
  WriteBatchInternal::SetCount(&batch, 1);
  LegacyBatchHandler handler;
  EXPECT_TRUE(batch.Iterate(&handler).IsCorruption());
  std::string key;
  AppendInternalKey(&key, ParsedInternalKey("key", 1, kTypeValue));
  key[key.size() - 8] = 3;
  ParsedInternalKey parsed;
  EXPECT_FALSE(ParseInternalKey(key, &parsed));
}

}  // namespace
}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
