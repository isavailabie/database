#include "leveldb/db.h"
#include <iostream>
#include <string>

using namespace std;
using namespace leveldb;

int main() {
    DB* db = nullptr;
    Options op;
    op.create_if_missing = true;
    Status status = DB::Open(op, "testdb", &db);
    if (!status.ok()) {
        cerr << "无法打开数据库: " << status.ToString() << endl;
        return 1;
    }

    // 写入大量数据以创建SST文件并触发Compaction
    cout << "开始写入大量数据..." << endl;
    for (int i = 0; i < 100000; ++i) {
        string key = "key" + to_string(i);
        string value = "value" + to_string(i);
        db->Put(WriteOptions(), key, value);
    }
    cout << "数据写入完成。" << endl;

    for (int i = 0; i < 10; ++i) {
        string key = "key" + to_string(i);
        string value;
        db->Get(ReadOptions(), key, &value);
        cout<< "数据读取 key: " << key << ",value: " << value << endl;
    }

    // 手动触发整个键范围的compaction
    cout << "手动触发 Compaction..." << endl;
    db->CompactRange(nullptr, nullptr);
    cout << "Compaction 完成" << endl;

    // 清理数据库并再次写入少量数据
    cout << "清理并重新写入数据..." << endl;
    for (int i = 0; i < 10000; ++i) {
        string key = "key" + to_string(i);
        db->Delete(WriteOptions(), key);
    }
    cout << "数据删除完成" << endl;
    
    // 再次手动触发 compaction，以清理被删除的键
    cout << "再次手动触发 Compaction 清理删除数据..." << endl;
    db->CompactRange(nullptr, nullptr);
    cout << "再次 Compaction 完成" << endl;

    delete db;
    return 0;
}