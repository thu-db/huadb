#pragma once

#include "catalog/column_list.h"
#include "common/types.h"
#include "log/log_manager.h"
#include "storage/buffer_pool.h"
#include "table/record.h"

namespace huadb {

class Table {
 public:
  Table(BufferPool &buffer_pool, LogManager &log_manager, oid_t oid, oid_t db_oid, ColumnList column_list,
        bool new_table, bool is_empty);

  // 插入记录，返回插入记录的 rid
  // write_log: 是否写日志。系统表操作不写日志，用户表操作写日志，lab 2 相关参数
  Rid InsertRecord(std::shared_ptr<Record> record, xid_t xid, cid_t cid, bool write_log);
  // 删除记录
  void DeleteRecord(const Rid &rid, xid_t xid, bool write_log);
  // 更新记录
  Rid UpdateRecord(const Rid &rid, xid_t xid, cid_t cid, std::shared_ptr<Record> record, bool write_log);

  // 用于系统表的原地更新，无需关注
  void UpdateRecordInPlace(const Record &record);

  // 获取表的第一个页面的页面号
  pageid_t GetFirstPageId() const;

  oid_t GetOid() const;
  oid_t GetDbOid() const;
  const ColumnList &GetColumnList() const;

 private:
  BufferPool &buffer_pool_;
  LogManager &log_manager_;
  oid_t oid_;
  oid_t db_oid_;
  pageid_t first_page_id_;  // 第一个页面的页面号
  ColumnList column_list_;  // 表的 schema 信息
};

}  // namespace huadb
