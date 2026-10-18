// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <limits>
#include <memory>
#include <vector>

#include "db/memtable.h"
#include "db/version_set.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

// Internal Env helper also used by filename.cc for atomic CURRENT updates.
Status WriteStringToFileSync(Env* env, const Slice& data,
                            const std::string& fname);

bool DBImpl::StatsCacheEligible() {
  mutex_.AssertHeld();
  if (mem_ == nullptr || imm_ != nullptr || !bg_error_.ok()) return false;
  std::unique_ptr<Iterator> it(mem_->NewIterator());
  it->SeekToFirst();
  // Never persist counters for records whose durable source is a WAL. WAL
  // recovery still takes the authoritative slow path, including reuse_logs.
  return !it->Valid() && it->status().ok();
}

std::string DBImpl::StatsFingerprint() {
  mutex_.AssertHeld();
  std::string fingerprint;
  PutLengthPrefixedSlice(&fingerprint, user_comparator_name_);
  PutFixed64(&fingerprint, versions_->LastSequence());
  for (int level = 0; level < config::kNumLevels; ++level) {
    std::vector<FileMetaData*> files;
    versions_->current()->GetOverlappingInputs(level, nullptr, nullptr, &files);
    PutFixed64(&fingerprint, files.size());
    for (const FileMetaData* file : files) {
      PutFixed64(&fingerprint, file->number);
      PutFixed64(&fingerprint, file->file_size);
      PutLengthPrefixedSlice(&fingerprint, file->smallest.Encode());
      PutLengthPrefixedSlice(&fingerprint, file->largest.Encode());
    }
  }
  return fingerprint;
}

bool DBImpl::LoadStatsCache() {
  mutex_.AssertHeld();
  const std::string path = dbname_ + "/STATS";
  std::string encoded;
  const bool eligible = StatsCacheEligible();
  const Status read = eligible ? ReadFileToString(env_, path, &encoded)
                               : Status::NotFound("WAL-backed statistics");
  // Cache consumption precedes publishing the DB. A subsequent crash cannot
  // leave a cache from before its writes/compactions; fingerprint is a second
  // independent stale-state check even if cache invalidation fails.
  env_->RemoveFile(path);
  env_->RemoveFile(dbname_ + "/STATS.tmp");
  if (!read.ok() || encoded.size() < 4) return false;
  const size_t payload_size = encoded.size() - 4;
  if (DecodeFixed32(encoded.data() + payload_size) !=
      crc32c::Value(encoded.data(), payload_size)) {
    return false;
  }
  Slice input(encoded.data(), payload_size);
  Slice magic, fingerprint;
  if (!GetLengthPrefixedSlice(&input, &magic) || magic != Slice("LDBSTATS1") ||
      !GetLengthPrefixedSlice(&input, &fingerprint) ||
      fingerprint != Slice(StatsFingerprint()) || input.size() < 16) {
    return false;
  }
  const uint64_t expected_total = DecodeFixed64(input.data());
  const uint64_t distinct = DecodeFixed64(input.data() + 8);
  input.remove_prefix(16);
  if (distinct > expected_total || distinct > input.size() / 9) return false;
  KeyCounts restored(UserKeyLess{user_comparator()});
  uint64_t total = 0;
  for (uint64_t i = 0; i < distinct; ++i) {
    Slice key;
    if (!GetLengthPrefixedSlice(&input, &key) || input.size() < 8) return false;
    const uint64_t count = DecodeFixed64(input.data());
    input.remove_prefix(8);
    if (count == 0 || count > std::numeric_limits<uint64_t>::max() - total ||
        (!restored.empty() &&
         user_comparator()->Compare(restored.rbegin()->first, key) >= 0)) {
      return false;
    }
    // Serialized user keys are ordered: a correct hint avoids N log N startup
    // comparisons while retaining support for custom comparator equivalence.
    restored.emplace_hint(restored.end(), key.ToString(), count);
    total += count;
  }
  if (!input.empty() || total != expected_total) return false;
  key_counts_.swap(restored);
  total_entries_ = total;
  stats_cache_loaded_ = true;
  return true;
}

void DBImpl::SaveStatsCache() {
  mutex_.AssertHeld();
  if (!stats_ready_ || !StatsCacheEligible()) return;
  std::string encoded;
  PutLengthPrefixedSlice(&encoded, "LDBSTATS1");
  PutLengthPrefixedSlice(&encoded, StatsFingerprint());
  PutFixed64(&encoded, total_entries_);
  PutFixed64(&encoded, key_counts_.size());
  for (const auto& entry : key_counts_) {
    PutLengthPrefixedSlice(&encoded, entry.first);
    PutFixed64(&encoded, entry.second);
  }
  PutFixed32(&encoded, crc32c::Value(encoded.data(), encoded.size()));
  const std::string temporary = dbname_ + "/STATS.tmp";
  Status status = WriteStringToFileSync(env_, encoded, temporary);
  if (status.ok()) status = env_->RenameFile(temporary, dbname_ + "/STATS");
  if (!status.ok()) {
    env_->RemoveFile(temporary);
    Log(options_.info_log, "optional stats cache write: %s",
        status.ToString().c_str());
  }
}

}  // namespace leveldb
