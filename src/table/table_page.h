#pragma once

#include <string>

#include "common/types.h"
#include "log/log_manager.h"
#include "storage/page.h"
#include "table/record.h"

namespace huadb {

// page_lsn(8) + next_page(4) + page_lower(2) + page_upper(2) = 16
static constexpr db_size_t PAGE_HEADER_SIZE = sizeof(lsn_t) + sizeof(pageid_t) + sizeof(db_size_t) + sizeof(db_size_t);

class ColumnList;

class TablePage {
 public:
  explicit TablePage(std::shared_ptr<Page> page);

  // 页面初始化
  void Init();

  // 插入记录，返回插入的槽号
  slotid_t InsertRecord(std::shared_ptr<Record> record, xid_t xid, cid_t cid);
  // 删除记录
  void DeleteRecord(slotid_t slot_id, xid_t xid);

  // 用于系统表的原地更新，无需关注
  void UpdateRecordInPlace(const Record &record, slotid_t slot_id);

  // 获取记录
  std::shared_ptr<Record> GetRecord(Rid rid, const ColumnList &column_list);

  // Lab 2: 回滚删除操作
  void UndoDeleteRecord(slotid_t slot_id);
  // Lab 2: 重做插入操作
  void RedoInsertRecord(slotid_t slot_id, char *raw_record, db_size_t page_offset, db_size_t record_size);

  // 获取记录数目
  db_size_t GetRecordCount() const;
  // Lab 2: 获取 page lsn
  lsn_t GetPageLSN() const;
  // 获取下一个页面的页面号
  pageid_t GetNextPageId() const;
  // 获取页面 lower 指针
  db_size_t GetLower() const;
  // 获取页面 upper 指针
  db_size_t GetUpper() const;

  // 获取页面剩余空间大小
  db_size_t GetFreeSpaceSize() const;

  // 设置下一个页面的页面号
  void SetNextPageId(pageid_t page_id);
  // Lab 2: 设置 page lsn
  void SetPageLSN(lsn_t page_lsn);

  std::string ToString() const;

 private:
  std::shared_ptr<Page> page_;
  char *page_data_;
  lsn_t *page_lsn_;         // LAB 2: PageLSN
  pageid_t *next_page_id_;  // 下一个页面的页面号
  db_size_t *lower_;        // 页面 lower 指针
  db_size_t *upper_;        // 页面 upper 指针
  Slot *slots_;             // 槽位数组
};

}  // namespace huadb
