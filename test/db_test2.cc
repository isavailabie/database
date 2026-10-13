#include "leveldb/db.h"
#include "leveldb/filter_policy.h"

#include <iostream>
#include <cstdlib>
#include <ctime>

using namespace leveldb;

constexpr int value_size = 2048;
constexpr int data_size = 256 << 20;

// 3. 数据管理（Manifest/创建/恢复数据库）
Status OpenDB(std::string dbName, DB **db) {
  Options options;
  options.create_if_missing = true;
  options.filter_policy = NewBloomFilterPolicy(10);
  return DB::Open(options, dbName, db);
}

// 1. 存储（数据结构与写入）
// 4. 数据合并（Compaction）
void InsertData(DB *db) {
  WriteOptions writeOptions;
  int key_num = data_size / value_size;
  srand(0);

  for (int i = 0; i < key_num; i++) {
    int key_ = rand() % key_num+1;
    std::string key = std::to_string(key_);
    std::string value(value_size, 'a');
    db->Put(writeOptions, key, value);
  }
}

// 2. 数据访问（如何读数据）
void GetData(DB *db, int size = (1 << 30)) {
  ReadOptions readOptions;
  int key_num = data_size / value_size;

  // 点查
  srand(0);
  for (int i = 0; i < 100; i++) {
    int key_ = rand() % key_num+1;
    std::string key = std::to_string(key_);
    std::string value;
    db->Get(readOptions, key, &value);
  }

  // 范围查询
  Iterator *iter = db->NewIterator(readOptions);
  iter->SeekToFirst();
  while (iter->Valid()) {
    iter->Next();
  }
  delete iter;
}

int main() {

  DB *db;
  if(OpenDB("testdb", &db).ok()) {
    InsertData(db);
    delete db;
  }

  if(OpenDB("testdb", &db).ok()) {
    GetData(db);
    delete db;
  }

  return 0;
}
