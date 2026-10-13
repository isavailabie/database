#include "gtest/gtest.h"
#include "leveldb/env.h"
#include "leveldb/db.h"
#include <unistd.h>   // getpid
#include <cstdio>     // snprintf
#include <string>

using namespace leveldb;

class StatsDBTest : public ::testing::Test {
 protected:
  std::string dbname_;
  DB* db_ = nullptr;

  void SetUp() override {
    const testing::TestInfo* info =
        testing::UnitTest::GetInstance()->current_test_info();
    // 生成唯一的 DB 目录名，降低并发/重复运行时冲突
    char buf[512];
    std::snprintf(buf, sizeof(buf), "testdb-%s-%s-%d-%lld",
                  info->test_suite_name(), info->name(), (int)getpid(),
                  static_cast<long long>(Env::Default()->NowMicros()));
    dbname_ = buf;

    // 确保干净环境
    Options opt;
    DestroyDB(dbname_, opt);

    // 打开数据库
    opt.create_if_missing = true;
    ASSERT_TRUE(DB::Open(opt, dbname_, &db_).ok());
  }

  void TearDown() override {
    // 先关闭，再销毁目录，彻底隔离
    if (db_ != nullptr) {
      delete db_;
      db_ = nullptr;
    }
    Options opt;
    DestroyDB(dbname_, opt);
  }
};

TEST_F(StatsDBTest, BasicCount) {
  ASSERT_NE(db_, nullptr);
  db_->Put(WriteOptions(), "k1", "v1");
  db_->Put(WriteOptions(), "k2", "v2");
  db_->Put(WriteOptions(), "k1", "v3");  // 重复 key

  Stats stats;
  db_->GetCurrentStats(ReadOptions(), &stats);

  ASSERT_EQ(stats.total_entries, 3);
  ASSERT_EQ(stats.distinct_keys, 2);
}

TEST_F(StatsDBTest, PersistAndRecover) {
  ASSERT_NE(db_, nullptr);
  db_->Put(WriteOptions(), "a", "1");
  db_->Put(WriteOptions(), "b", "2");

  Stats before;
  db_->GetCurrentStats(ReadOptions(), &before);

  // 模拟重启：关闭并重新打开同一 DB 目录
  delete db_;
  db_ = nullptr;

  Options opt;
  opt.create_if_missing = false;  // 期望已有目录
  ASSERT_TRUE(DB::Open(opt, dbname_, &db_).ok());

  Stats after;
  db_->GetCurrentStats(ReadOptions(), &after);

  ASSERT_EQ(before.total_entries, after.total_entries);
  ASSERT_EQ(before.distinct_keys, after.distinct_keys);
}

TEST_F(StatsDBTest, CompactionUpdate) {
  ASSERT_NE(db_, nullptr);

  for (int i = 0; i < 10000; ++i) {
    db_->Put(WriteOptions(), "k" + std::to_string(i), "v");
  }

  db_->CompactRange(nullptr, nullptr);

  Stats st;
  db_->GetCurrentStats(ReadOptions(), &st);

  ASSERT_GT(st.total_entries, 0);
  ASSERT_GT(st.distinct_keys, 0);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}