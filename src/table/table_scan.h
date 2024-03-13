#pragma once

#include <unordered_map>

#include "common/types.h"
#include "storage/buffer_pool.h"
#include "table/record.h"
#include "table/table.h"

namespace huadb {

class TableScan {
 public:
  TableScan(BufferPool &buffer_pool, std::shared_ptr<Table> table, Rid rid);
  // xid: 事务 id
  // isolation_level: 隔离级别
  // cid: 事物内部 command id
  // active_xids: 活跃的事务 id 集合
  // 均为 Lab 3 相关参数
  std::shared_ptr<Record> GetNextRecord(xid_t xid = NULL_XID, IsolationLevel isolation_level = DEFAULT_ISOLATION_LEVEL,
                                        cid_t cid = NULL_CID, const std::unordered_set<xid_t> &active_xids = {});

 private:
  BufferPool &buffer_pool_;
  std::shared_ptr<Table> table_;
  Rid rid_;  // 当前扫描到的记录的 rid
};

}  // namespace huadb
