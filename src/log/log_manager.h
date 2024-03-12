#pragma once

#include <atomic>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "catalog/catalog.h"
#include "common/constants.h"
#include "log/log_record.h"
#include "storage/buffer_pool.h"
#include "storage/disk.h"
#include "transaction/transaction_manager.h"

namespace huadb {

class LogManager {
 public:
  LogManager(Disk &disk, TransactionManager &transaction_manager, lsn_t next_lsn);

  void SetBufferPool(std::shared_ptr<BufferPool> buffer_pool);
  void SetCatalog(std::shared_ptr<Catalog> catalog);

  lsn_t GetNextLSN() const;

  // 清空 log buffer，用于数据库故障模拟
  void Clear();

  // 将日志缓存刷盘
  void Flush();

  // 将 {oid, page_id} 添加到脏页表
  void SetDirty(oid_t oid, pageid_t page_id, lsn_t lsn);

  // 追加日志
  lsn_t AppendInsertLog(xid_t xid, oid_t oid, pageid_t page_id, slotid_t slot_id, db_size_t offset, db_size_t size,
                        char *new_record);
  lsn_t AppendDeleteLog(xid_t xid, oid_t oid, pageid_t page_id, slotid_t slot_id);
  lsn_t AppendNewPageLog(xid_t xid, oid_t oid, pageid_t prev_page_id, pageid_t page_id);
  lsn_t AppendBeginLog(xid_t xid);
  lsn_t AppendCommitLog(xid_t xid);
  lsn_t AppendRollbackLog(xid_t xid);

  // async: 是否异步刷盘（高级功能）
  lsn_t Checkpoint(bool async = false);

  // 刷脏页，需维护脏页表
  void FlushPage(oid_t table_oid, pageid_t page_id, lsn_t page_lsn);

  // 回滚单个事务
  void Rollback(xid_t xid);

  // 故障恢复
  void Recover();

  // Redo 次数递增
  void IncrementRedoCount();
  // Redo 次数统计
  uint32_t GetRedoCount() const;

 private:
  // 将 lsn 之前的日志刷到磁盘
  void Flush(lsn_t lsn);

  // ARIES 相关函数
  // 分析阶段，恢复脏页表和活跃事务表
  void Analyze();
  // 重做阶段，恢复未刷盘的脏页
  void Redo();
  // 恢复阶段，回滚所有活跃事务
  void Undo();

  Disk &disk_;
  TransactionManager &transaction_manager_;
  std::shared_ptr<BufferPool> buffer_pool_;
  std::shared_ptr<Catalog> catalog_;

  std::unordered_map<xid_t, lsn_t> att_;        // 活跃事务表
  std::unordered_map<TablePageid, lsn_t> dpt_;  // 脏页表

  // 下一条日志的 lsn
  std::atomic<lsn_t> next_lsn_;
  // 已经刷到磁盘的最大 lsn
  lsn_t flushed_lsn_;

  std::list<std::shared_ptr<LogRecord>> log_buffer_;
  std::shared_mutex log_buffer_mutex_;

  uint32_t redo_count_ = 0;
};

}  // namespace huadb
