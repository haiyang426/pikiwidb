//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_H_
#define SRC_REDIS_H_

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

#include "log_index.h"
#include "pstd/env.h"
#include "pstd/log.h"
#include "src/custom_comparator.h"
#include "src/debug.h"
#include "src/lock_mgr.h"
#include "src/lru_cache.h"
#include "src/mutex_impl.h"
#include "src/type_iterator.h"
#include "storage/storage.h"
#include "storage/storage_define.h"

#define SPOP_COMPACT_THRESHOLD_COUNT 500
#define SPOP_COMPACT_THRESHOLD_DURATION (1000 * 1000)  // 1000ms

namespace storage {
using Status = rocksdb::Status;
using Slice = rocksdb::Slice;

class Redis {
 public:
  Redis(Storage* storage, int32_t index);
  virtual ~Redis();

  rocksdb::DB* GetDB() { return db_; }

  struct KeyStatistics {
    size_t window_size;
    std::deque<uint64_t> durations;

    uint64_t modify_count;

    KeyStatistics() : KeyStatistics(10) {}

    KeyStatistics(size_t size) : window_size(size + 2), modify_count(0) {}

    void AddDuration(uint64_t duration) {
      durations.push_back(duration);
      while (durations.size() > window_size) {
        durations.pop_front();
      }
    }
    uint64_t AvgDuration() {
      if (durations.size() < window_size) {
        return 0;
      }
      uint64_t min = durations[0];
      uint64_t max = durations[0];
      uint64_t sum = 0;
      for (auto duration : durations) {
        if (duration < min) {
          min = duration;
        }
        if (duration > max) {
          max = duration;
        }
        sum += duration;
      }
      return (sum - max - min) / (durations.size() - 2);
    }
    void AddModifyCount(uint64_t count) { modify_count += count; }
    uint64_t ModifyCount() { return modify_count; }
  };

  struct KeyStatisticsDurationGuard {
    Redis* ctx;
    std::string key;
    uint64_t start_us;
    DataType dtype;
    KeyStatisticsDurationGuard(Redis* that, const DataType type, const std::string& key)
        : ctx(that), key(key), start_us(pstd::NowMicros()), dtype(type) {}
    ~KeyStatisticsDurationGuard() {
      uint64_t end_us = pstd::NowMicros();
      uint64_t duration = end_us > start_us ? end_us - start_us : 0;
      ctx->UpdateSpecificKeyDuration(dtype, key, duration);
    }
  };

  int GetIndex() const { return index_; }

  Status SetOptions(const OptionType& option_type, const std::unordered_map<std::string, std::string>& options);
  void SetWriteWalOptions(const bool is_wal_disable);

  // Common Commands
  Status Open(const StorageOptions& storage_options, const std::string& db_path);

  void SetNeedClose(bool need_close) { need_close_.store(need_close); }

  virtual Status CompactRange(const DataType& option_type, const rocksdb::Slice* begin, const rocksdb::Slice* end,
                              const ColumnFamilyType& type = kMetaAndData);

  virtual Status GetProperty(const std::string& property, uint64_t* out);
  bool IsApplied(size_t cf_idx, LogIndex logidx) const { return log_index_of_all_cfs_.IsApplied(cf_idx, logidx); }
  void UpdateAppliedLogIndexOfColumnFamily(size_t cf_idx, LogIndex logidx, SequenceNumber seqno) {
    log_index_of_all_cfs_.Update(cf_idx, logidx, seqno);
  }
  bool IsRestarting() const { return is_starting_; }
  void StartingPhaseEnd() { is_starting_ = false; }

  Status ScanKeyNum(std::vector<KeyInfo>* key_info);
  Status ScanStringsKeyNum(KeyInfo* key_info);
  Status ScanHashesKeyNum(KeyInfo* key_info);
  Status ScanListsKeyNum(KeyInfo* key_info);
  Status ScanZsetsKeyNum(KeyInfo* key_info);
  Status ScanSetsKeyNum(KeyInfo* key_info);

  virtual Status StringsPKPatternMatchDel(const std::string& pattern, int32_t* ret);
  virtual Status ListsPKPatternMatchDel(const std::string& pattern, int32_t* ret);
  virtual Status HashesPKPatternMatchDel(const std::string& pattern, int32_t* ret);
  virtual Status ZsetsPKPatternMatchDel(const std::string& pattern, int32_t* ret);
  virtual Status SetsPKPatternMatchDel(const std::string& pattern, int32_t* ret);

  // Keys Commands
  virtual Status StringsExpire(const Slice& key, uint64_t ttl);
  virtual Status HashesExpire(const Slice& key, uint64_t ttl);
  virtual Status ListsExpire(const Slice& key, uint64_t ttl);
  virtual Status ZsetsExpire(const Slice& key, uint64_t ttl);
  virtual Status SetsExpire(const Slice& key, uint64_t ttl);

  virtual Status StringsDel(const Slice& key);
  virtual Status HashesDel(const Slice& key);
  virtual Status ListsDel(const Slice& key);
  virtual Status ZsetsDel(const Slice& key);
  virtual Status SetsDel(const Slice& key);

  virtual Status StringsExpireat(const Slice& key, uint64_t timestamp);
  virtual Status HashesExpireat(const Slice& key, uint64_t timestamp);
  virtual Status ListsExpireat(const Slice& key, uint64_t timestamp);
  virtual Status SetsExpireat(const Slice& key, uint64_t timestamp);
  virtual Status ZsetsExpireat(const Slice& key, uint64_t timestamp);

  virtual Status StringsPersist(const Slice& key);
  virtual Status HashesPersist(const Slice& key);
  virtual Status ListsPersist(const Slice& key);
  virtual Status ZsetsPersist(const Slice& key);
  virtual Status SetsPersist(const Slice& key);

  virtual Status StringsTTL(const Slice& key, uint64_t* timestamp);
  virtual Status HashesTTL(const Slice& key, uint64_t* timestamp);
  virtual Status ListsTTL(const Slice& key, uint64_t* timestamp);
  virtual Status ZsetsTTL(const Slice& key, uint64_t* timestamp);
  virtual Status SetsTTL(const Slice& key, uint64_t* timestamp);

  virtual Status StringsRename(const Slice& key, Redis* new_inst, const Slice& newkey);
  virtual Status HashesRename(const Slice& key, Redis* new_inst, const Slice& newkey);
  virtual Status ListsRename(const Slice& key, Redis* new_inst, const Slice& newkey);
  virtual Status ZsetsRename(const Slice& key, Redis* new_inst, const Slice& newkey);
  virtual Status SetsRename(const Slice& key, Redis* new_inst, const Slice& newkey);

  virtual Status StringsRenamenx(const Slice& key, Redis* new_inst, const Slice& newkey);
  virtual Status HashesRenamenx(const Slice& key, Redis* new_inst, const Slice& newkey);
  virtual Status ListsRenamenx(const Slice& key, Redis* new_inst, const Slice& newkey);
  virtual Status ZsetsRenamenx(const Slice& key, Redis* new_inst, const Slice& newkey);
  virtual Status SetsRenamenx(const Slice& key, Redis* new_inst, const Slice& newkey);

  // Strings Commands
  Status Append(const Slice& key, const Slice& value, int32_t* ret);
  Status BitCount(const Slice& key, int64_t start_offset, int64_t end_offset, int32_t* ret, bool have_range);
  Status BitOp(BitOpType op, const std::string& dest_key, const std::vector<std::string>& src_keys,
               std::string& value_to_dest, int64_t* ret);
  Status Decrby(const Slice& key, int64_t value, int64_t* ret);
  Status Get(const Slice& key, std::string* value);
  Status GetWithTTL(const Slice& key, std::string* value, uint64_t* ttl);
  Status GetBit(const Slice& key, int64_t offset, int32_t* ret);
  Status Getrange(const Slice& key, int64_t start_offset, int64_t end_offset, std::string* ret);
  Status GetrangeWithValue(const Slice& key, int64_t start_offset, int64_t end_offset, std::string* ret,
                           std::string* value, uint64_t* ttl);
  Status GetSet(const Slice& key, const Slice& value, std::string* old_value);
  Status Incrby(const Slice& key, int64_t value, int64_t* ret);
  Status Incrbyfloat(const Slice& key, const Slice& value, std::string* ret);
  Status MSet(const std::vector<KeyValue>& kvs);
  Status MSetnx(const std::vector<KeyValue>& kvs, int32_t* ret);
  Status Set(const Slice& key, const Slice& value);
  Status Setxx(const Slice& key, const Slice& value, int32_t* ret, uint64_t ttl = 0);
  Status SetBit(const Slice& key, int64_t offset, int32_t value, int32_t* ret);
  Status Setex(const Slice& key, const Slice& value, uint64_t ttl);
  Status Setnx(const Slice& key, const Slice& value, int32_t* ret, uint64_t ttl = 0);
  Status Setvx(const Slice& key, const Slice& value, const Slice& new_value, int32_t* ret, uint64_t ttl = 0);
  Status Delvx(const Slice& key, const Slice& value, int32_t* ret);
  Status Setrange(const Slice& key, int64_t start_offset, const Slice& value, int32_t* ret);
  Status Strlen(const Slice& key, int32_t* len);

  Status BitPos(const Slice& key, int32_t bit, int64_t* ret);
  Status BitPos(const Slice& key, int32_t bit, int64_t start_offset, int64_t* ret);
  Status BitPos(const Slice& key, int32_t bit, int64_t start_offset, int64_t end_offset, int64_t* ret);
  Status PKSetexAt(const Slice& key, const Slice& value, uint64_t timestamp);

  // Hash Commands
  Status HDel(const Slice& key, const std::vector<std::string>& fields, int32_t* ret);
  Status HExists(const Slice& key, const Slice& field);
  Status HGet(const Slice& key, const Slice& field, std::string* value);
  Status HGetall(const Slice& key, std::vector<FieldValue>* fvs);
  Status HGetallWithTTL(const Slice& key, std::vector<FieldValue>* fvs, uint64_t* ttl);
  Status HIncrby(const Slice& key, const Slice& field, int64_t value, int64_t* ret);
  Status HIncrbyfloat(const Slice& key, const Slice& field, const Slice& by, std::string* new_value);
  Status HKeys(const Slice& key, std::vector<std::string>* fields);
  Status HLen(const Slice& key, int32_t* ret);
  Status HMGet(const Slice& key, const std::vector<std::string>& fields, std::vector<ValueStatus>* vss);
  Status HMSet(const Slice& key, const std::vector<FieldValue>& fvs);
  Status HSet(const Slice& key, const Slice& field, const Slice& value, int32_t* res);
  Status HSetnx(const Slice& key, const Slice& field, const Slice& value, int32_t* ret);
  Status HVals(const Slice& key, std::vector<std::string>* values);
  Status HStrlen(const Slice& key, const Slice& field, int32_t* len);
  Status HScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
               std::vector<FieldValue>* field_values, int64_t* next_cursor);
  Status HScanx(const Slice& key, const std::string& start_field, const std::string& pattern, int64_t count,
                std::vector<FieldValue>* field_values, std::string* next_field);
  Status HRandField(const Slice& key, int64_t count, bool with_values, std::vector<std::string>* res);
  Status PKHScanRange(const Slice& key, const Slice& field_start, const std::string& field_end, const Slice& pattern,
                      int32_t limit, std::vector<FieldValue>* field_values, std::string* next_field);
  Status PKHRScanRange(const Slice& key, const Slice& field_start, const std::string& field_end, const Slice& pattern,
                       int32_t limit, std::vector<FieldValue>* field_values, std::string* next_field);

  Status SetMaxCacheStatisticKeys(size_t max_cache_statistic_keys);
  Status SetSmallCompactionThreshold(uint64_t small_compaction_threshold);
  Status SetSmallCompactionDurationThreshold(uint64_t small_compaction_duration_threshold);
  void GetRocksDBInfo(std::string& info, const char* prefix);
  auto GetWriteOptions() const -> const rocksdb::WriteOptions& { return default_write_options_; }
  auto GetColumnFamilyHandles() const -> const std::vector<rocksdb::ColumnFamilyHandle*>& { return handles_; }
  auto GetRaftTimeout() const -> uint32_t { return raft_timeout_s_; }
  auto GetAppendLogFunction() const -> const AppendLogFunction& { return append_log_function_; }

  // Sets Commands
  Status SAdd(const Slice& key, const std::vector<std::string>& members, int32_t* ret);
  Status SCard(const Slice& key, int32_t* ret);
  Status SDiff(const std::vector<std::string>& keys, std::vector<std::string>* members);
  Status SDiffstore(const Slice& destination, const std::vector<std::string>& keys,
                    std::vector<std::string>& value_to_dest, int32_t* ret);
  Status SInter(const std::vector<std::string>& keys, std::vector<std::string>* members);
  Status SInterstore(const Slice& destination, const std::vector<std::string>& keys,
                     std::vector<std::string>& value_to_dest, int32_t* ret);
  Status SIsmember(const Slice& key, const Slice& member, int32_t* ret);
  Status SMembers(const Slice& key, std::vector<std::string>* members);
  Status SMembersWithTTL(const Slice& key, std::vector<std::string>* members, uint64_t* ttl);
  Status SMove(const Slice& source, const Slice& destination, const Slice& member, int32_t* ret);
  Status SPop(const Slice& key, std::vector<std::string>* members, int64_t cnt);
  Status SRandmember(const Slice& key, int32_t count, std::vector<std::string>* members);
  Status SRem(const Slice& key, const std::vector<std::string>& members, int32_t* ret);
  Status SUnion(const std::vector<std::string>& keys, std::vector<std::string>* members);
  Status SUnionstore(const Slice& destination, const std::vector<std::string>& keys,
                     std::vector<std::string>& value_to_dest, int32_t* ret);
  Status SScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
               std::vector<std::string>* members, int64_t* next_cursor);
  Status AddAndGetSpopCount(const std::string& key, uint64_t* count);
  Status ResetSpopCount(const std::string& key);

  // Lists commands
  Status LIndex(const Slice& key, int64_t index, std::string* element);
  Status LInsert(const Slice& key, const BeforeOrAfter& before_or_after, const std::string& pivot,
                 const std::string& value, int64_t* ret);
  Status LLen(const Slice& key, uint64_t* len);
  Status LPop(const Slice& key, int64_t count, std::vector<std::string>* elements);
  Status LPush(const Slice& key, const std::vector<std::string>& values, uint64_t* ret);
  Status LPushx(const Slice& key, const std::vector<std::string>& values, uint64_t* len);
  Status LRange(const Slice& key, int64_t start, int64_t stop, std::vector<std::string>* ret);
  Status LRangeWithTTL(const Slice& key, int64_t start, int64_t stop, std::vector<std::string>* ret, uint64_t* ttl);
  Status LRem(const Slice& key, int64_t count, const Slice& value, uint64_t* ret);
  Status LSet(const Slice& key, int64_t index, const Slice& value);
  Status LTrim(const Slice& key, int64_t start, int64_t stop);
  Status RPop(const Slice& key, int64_t count, std::vector<std::string>* elements);
  Status RPoplpush(const Slice& source, const Slice& destination, std::string* element);
  Status RPush(const Slice& key, const std::vector<std::string>& values, uint64_t* ret);
  Status RPushx(const Slice& key, const std::vector<std::string>& values, uint64_t* len);

  // Zsets Commands
  Status ZAdd(const Slice& key, const std::vector<ScoreMember>& score_members, int32_t* ret);
  Status ZCard(const Slice& key, int32_t* card);
  Status ZCount(const Slice& key, double min, double max, bool left_close, bool right_close, int32_t* ret);
  Status ZIncrby(const Slice& key, const Slice& member, double increment, double* ret);
  Status ZRange(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members);
  Status ZRangeWithTTL(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members,
                       uint64_t* ttl);
  Status ZRangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close, int64_t count,
                       int64_t offset, std::vector<ScoreMember>* score_members);
  Status ZRank(const Slice& key, const Slice& member, int32_t* rank);
  Status ZRem(const Slice& key, const std::vector<std::string>& members, int32_t* ret);
  Status ZRemrangebyrank(const Slice& key, int32_t start, int32_t stop, int32_t* ret);
  Status ZRemrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close, int32_t* ret);
  Status ZRevrange(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members);
  Status ZRevrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close, int64_t count,
                          int64_t offset, std::vector<ScoreMember>* score_members);
  Status ZRevrank(const Slice& key, const Slice& member, int32_t* rank);
  Status ZScore(const Slice& key, const Slice& member, double* score);
  Status ZGetAll(const Slice& key, double weight, std::map<std::string, double>* value_to_dest);
  Status ZUnionstore(const Slice& destination, const std::vector<std::string>& keys, const std::vector<double>& weights,
                     AGGREGATE agg, std::map<std::string, double>& value_to_dest, int32_t* ret);
  Status ZInterstore(const Slice& destination, const std::vector<std::string>& keys, const std::vector<double>& weights,
                     AGGREGATE agg, std::vector<ScoreMember>& value_to_dest, int32_t* ret);
  Status ZRangebylex(const Slice& key, const Slice& min, const Slice& max, bool left_close, bool right_close,
                     std::vector<std::string>* members);
  Status ZLexcount(const Slice& key, const Slice& min, const Slice& max, bool left_close, bool right_close,
                   int32_t* ret);
  Status ZRemrangebylex(const Slice& key, const Slice& min, const Slice& max, bool left_close, bool right_close,
                        int32_t* ret);
  Status ZScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
               std::vector<ScoreMember>* score_members, int64_t* next_cursor);
  Status ZPopMax(const Slice& key, int64_t count, std::vector<ScoreMember>* score_members);
  Status ZPopMin(const Slice& key, int64_t count, std::vector<ScoreMember>* score_members);

  void ScanDatabase();
  void ScanStrings();
  void ScanHashes();
  void ScanLists();
  void ScanZsets();
  void ScanSets();

  void UpdateLogIndex(LogIndex applied_log_index, SequenceNumber seqno) {
    log_index_collector_.Update(applied_log_index, seqno);
  }

  TypeIterator* CreateIterator(const DataType& type, const std::string& pattern, const Slice* lower_bound,
                               const Slice* upper_bound) {
    return CreateIterator(DataTypeTag[type], pattern, lower_bound, upper_bound);
  }

  TypeIterator* CreateIterator(const char& type, const std::string& pattern, const Slice* lower_bound,
                               const Slice* upper_bound) {
    rocksdb::ReadOptions options;
    options.fill_cache = false;
    options.iterate_lower_bound = lower_bound;
    options.iterate_upper_bound = upper_bound;
    switch (type) {
      case 'k':
        return new StringsIterator(options, db_, handles_[kStringsCF], pattern);
        break;
      case 'h':
        return new HashesIterator(options, db_, handles_[kHashesMetaCF], pattern);
        break;
      case 's':
        return new SetsIterator(options, db_, handles_[kSetsMetaCF], pattern);
        break;
      case 'l':
        return new ListsIterator(options, db_, handles_[kListsMetaCF], pattern);
        break;
      case 'z':
        return new ZsetsIterator(options, db_, handles_[kZsetsMetaCF], pattern);
        break;
      default:
        WARN("Invalid datatype to create iterator");
        return nullptr;
    }
    return nullptr;
  }

  LogIndexOfColumnFamilies& GetLogIndexOfColumnFamilies() { return log_index_of_all_cfs_; }

  LogIndexAndSequenceCollector& GetCollector() { return log_index_collector_; }

 private:
  int32_t index_ = 0;
  std::atomic<bool> need_close_ = false;
  Storage* const storage_;
  std::shared_ptr<LockMgr> lock_mgr_;
  rocksdb::DB* db_ = nullptr;

  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
  rocksdb::WriteOptions default_write_options_;
  rocksdb::ReadOptions default_read_options_;
  rocksdb::CompactRangeOptions default_compact_range_options_;

  // For Scan
  std::unique_ptr<LRUCache<std::string, std::string>> scan_cursors_store_;
  std::unique_ptr<LRUCache<std::string, size_t>> spop_counts_store_;

  Status GetScanStartPoint(const DataType& type, const Slice& key, const Slice& pattern, int64_t cursor,
                           std::string* start_point);
  Status StoreScanNextPoint(const DataType& type, const Slice& key, const Slice& pattern, int64_t cursor,
                            const std::string& next_point);

  // For Statistics
  std::atomic_uint64_t small_compaction_threshold_;
  std::atomic_uint64_t small_compaction_duration_threshold_;
  std::unique_ptr<LRUCache<std::string, KeyStatistics>> statistics_store_;

  // For raft
  uint32_t raft_timeout_s_ = 10;
  AppendLogFunction append_log_function_;
  LogIndexAndSequenceCollector log_index_collector_;
  LogIndexOfColumnFamilies log_index_of_all_cfs_;
  bool is_starting_{true};

  Status UpdateSpecificKeyStatistics(const DataType& dtype, const std::string& key, uint64_t count);
  Status UpdateSpecificKeyDuration(const DataType& dtype, const std::string& key, uint64_t duration);
  Status AddCompactKeyTaskIfNeeded(const DataType& dtype, const std::string& key, uint64_t count, uint64_t duration);
};

}  //  namespace storage
#endif  //  SRC_REDIS_H_
