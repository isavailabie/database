// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/version_set.h"
#include "util/coding.h"

namespace leveldb {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9,
  kFormatVersion = 10,
  kStatsSnapshotTotal = 11,
  kStatsSnapshotEntry = 12,
  kStatsDelta = 13,
  kStatsDeltaMarker = 14
};

void VersionEdit::Clear() {
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  format_version_ = 0;
  stats_snapshot_total_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  has_format_version_ = false;
  has_stats_snapshot_ = false;
  has_stats_delta_ = false;
  compact_pointers_.clear();
  stats_snapshot_.clear();
  stats_deltas_.clear();
  deleted_files_.clear();
  new_files_.clear();
}

void VersionEdit::EncodeTo(std::string* dst) const {
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32(dst, kPrevLogNumber);
    PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }
  if (has_format_version_) {
    PutVarint32(dst, kFormatVersion);
    PutVarint32(dst, format_version_);
  }
  if (has_stats_snapshot_) {
    PutVarint32(dst, kStatsSnapshotTotal);
    PutVarint64(dst, stats_snapshot_total_);
    for (const auto& entry : stats_snapshot_) {
      PutVarint32(dst, kStatsSnapshotEntry);
      PutLengthPrefixedSlice(dst, entry.first);
      PutVarint64(dst, entry.second);
    }
  }
  for (const StatsDelta& delta : stats_deltas_) {
    PutVarint32(dst, kStatsDelta);
    PutLengthPrefixedSlice(dst, delta.key);
    PutVarint32(dst, delta.add ? 1 : 0);
    PutVarint64(dst, delta.count);
  }
  if (has_stats_delta_ && stats_deltas_.empty()) {
    PutVarint32(dst, kStatsDeltaMarker);
  }

  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    PutVarint32(dst, kCompactPointer);
    PutVarint32(dst, compact_pointers_[i].first);  // level
    PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());
  }

  for (const auto& deleted_file_kvp : deleted_files_) {
    PutVarint32(dst, kDeletedFile);
    PutVarint32(dst, deleted_file_kvp.first);   // level
    PutVarint64(dst, deleted_file_kvp.second);  // file number
  }

  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    PutVarint32(dst, kNewFile);
    PutVarint32(dst, new_files_[i].first);  // level
    PutVarint64(dst, f.number);
    PutVarint64(dst, f.file_size);
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());
  }
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    return dst->DecodeFrom(str);
  } else {
    return false;
  }
}

static bool GetLevel(Slice* input, int* level) {
  uint32_t v;
  if (GetVarint32(input, &v) && v < config::kNumLevels) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

Status VersionEdit::DecodeFrom(const Slice& src) {
  Clear();
  Slice input = src;
  const char* msg = nullptr;
  uint32_t tag;

  // Temporary storage for parsing
  int level;
  uint64_t number;
  FileMetaData f;
  Slice str;
  InternalKey key;

  while (msg == nullptr && GetVarint32(&input, &tag)) {
    switch (tag) {
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kFormatVersion:
        if (GetVarint32(&input, &format_version_)) {
          has_format_version_ = true;
        } else {
          msg = "format version";
        }
        break;

      case kStatsSnapshotTotal:
        if (!has_stats_snapshot_ &&
            GetVarint64(&input, &stats_snapshot_total_)) {
          has_stats_snapshot_ = true;
        } else {
          msg = "stats snapshot total";
        }
        break;

      case kStatsSnapshotEntry: {
        uint64_t count;
        if (GetLengthPrefixedSlice(&input, &str) &&
            GetVarint64(&input, &count) && count != 0) {
          stats_snapshot_.emplace_back(str.ToString(), count);
        } else {
          msg = "stats snapshot entry";
        }
        break;
      }

      case kStatsDelta: {
        uint32_t add;
        uint64_t count;
        if (GetLengthPrefixedSlice(&input, &str) &&
            GetVarint32(&input, &add) && add <= 1 &&
            GetVarint64(&input, &count) && count != 0) {
          stats_deltas_.push_back({str.ToString(), count, add != 0});
          has_stats_delta_ = true;
        } else {
          msg = "stats delta";
        }
        break;
      }

      case kStatsDeltaMarker:
        if (has_stats_delta_) {
          msg = "duplicate stats delta marker";
        } else {
          has_stats_delta_ = true;
        }
        break;

      case kCompactPointer:
        if (GetLevel(&input, &level) && GetInternalKey(&input, &key)) {
          compact_pointers_.push_back(std::make_pair(level, key));
        } else {
          msg = "compaction pointer";
        }
        break;

      case kDeletedFile:
        if (GetLevel(&input, &level) && GetVarint64(&input, &number)) {
          deleted_files_.insert(std::make_pair(level, number));
        } else {
          msg = "deleted file";
        }
        break;

      case kNewFile:
        if (GetLevel(&input, &level) && GetVarint64(&input, &f.number) &&
            GetVarint64(&input, &f.file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          new_files_.push_back(std::make_pair(level, f));
        } else {
          msg = "new-file entry";
        }
        break;

      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }
  if (msg == nullptr && !has_stats_snapshot_ && !stats_snapshot_.empty()) {
    msg = "stats snapshot entries without total";
  }

  Status result;
  if (msg != nullptr) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString() const {
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFile: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  if (has_format_version_) {
    r.append("\n  FormatVersion: ");
    AppendNumberTo(&r, format_version_);
  }
  if (has_stats_snapshot_) {
    r.append("\n  StatsSnapshotTotal: ");
    AppendNumberTo(&r, stats_snapshot_total_);
    r.append("\n  StatsSnapshotEntries: ");
    AppendNumberTo(&r, stats_snapshot_.size());
  }
  if (!stats_deltas_.empty()) {
    r.append("\n  StatsDeltas: ");
    AppendNumberTo(&r, stats_deltas_.size());
  }
  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    r.append("\n  CompactPointer: ");
    AppendNumberTo(&r, compact_pointers_[i].first);
    r.append(" ");
    r.append(compact_pointers_[i].second.DebugString());
  }
  for (const auto& deleted_files_kvp : deleted_files_) {
    r.append("\n  RemoveFile: ");
    AppendNumberTo(&r, deleted_files_kvp.first);
    r.append(" ");
    AppendNumberTo(&r, deleted_files_kvp.second);
  }
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    r.append("\n  AddFile: ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f.number);
    r.append(" ");
    AppendNumberTo(&r, f.file_size);
    r.append(" ");
    r.append(f.smallest.DebugString());
    r.append(" .. ");
    r.append(f.largest.DebugString());
  }
  r.append("\n}\n");
  return r;
}

}  // namespace leveldb
