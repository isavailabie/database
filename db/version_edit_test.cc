// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "gtest/gtest.h"
#include "util/coding.h"

namespace leveldb {

static void TestEncodeDecode(const VersionEdit& edit) {
  std::string encoded, encoded2;
  edit.EncodeTo(&encoded);
  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  parsed.EncodeTo(&encoded2);
  ASSERT_EQ(encoded, encoded2);
}

TEST(VersionEditTest, EncodeDecode) {
  static const uint64_t kBig = 1ull << 50;

  VersionEdit edit;
  for (int i = 0; i < 4; i++) {
    TestEncodeDecode(edit);
    edit.AddFile(3, kBig + 300 + i, kBig + 400 + i,
                 InternalKey("foo", kBig + 500 + i, kTypeValue),
                 InternalKey("zoo", kBig + 600 + i, kTypeDeletion));
    edit.RemoveFile(4, kBig + 700 + i);
    edit.SetCompactPointer(i, InternalKey("x", kBig + 900 + i, kTypeValue));
  }

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  edit.SetFormatVersion(kCurrentDatabaseFormatVersion);
  edit.SetStatsSnapshot(
      3, {{"a", 2}, {"b", 1}});
  edit.AddStatsDelta("c", 2, true);
  edit.AddStatsDelta("a", 1, false);
  TestEncodeDecode(edit);
}

TEST(VersionEditTest, LegacyEditsRemainReadableAndFutureVersionsRoundTrip) {
  VersionEdit legacy;
  legacy.SetComparatorName("bytewise");
  std::string encoded;
  legacy.EncodeTo(&encoded);
  VersionEdit decoded;
  ASSERT_TRUE(decoded.DecodeFrom(encoded).ok());

  VersionEdit future;
  future.SetFormatVersion(kCurrentDatabaseFormatVersion + 1);
  TestEncodeDecode(future);
}

TEST(VersionEditTest, StatsSnapshotEntriesRequireSnapshotTotal) {
  std::string encoded;
  PutVarint32(&encoded, 12);  // kStatsSnapshotEntry
  PutLengthPrefixedSlice(&encoded, "orphan");
  PutVarint64(&encoded, 1);

  VersionEdit decoded;
  EXPECT_TRUE(decoded.DecodeFrom(encoded).IsCorruption());
}

}  // namespace leveldb
