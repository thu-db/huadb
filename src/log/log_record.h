#pragma once

#include <cassert>
#include <cstring>
#include <memory>

#include "catalog/catalog.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "storage/buffer_pool.h"

namespace huadb {

enum class LogType : uint8_t {
  BEGIN,
  COMMIT,
  ROLLBACK,
  INSERT,
  DELETE,
  UPDATE,
  NEW_PAGE,
  BEGIN_CHECKPOINT,
  END_CHECKPOINT,
};

class LogRecord {
 public:
  LogRecord(LogType type, xid_t xid, lsn_t prev_lsn);

  // 序列化和反序列化
  virtual size_t SerializeTo(char *data) const;
  static std::shared_ptr<LogRecord> DeserializeFrom(const char *data);

  // 撤销和重做
  virtual void Undo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager, lsn_t lsn, lsn_t undo_next_lsn);
  virtual void Redo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager, lsn_t lsn);

  // 设置 lsn
  void SetLSN(lsn_t lsn);

  // 获取日志类型
  LogType GetType() const;

  lsn_t GetLSN() const;
  xid_t GetXid() const;
  lsn_t GetPrevLSN() const;

  // 获取日志大小
  size_t GetSize() const;

 protected:
  LogType type_;

  lsn_t lsn_;
  xid_t xid_;
  lsn_t prev_lsn_;

  uint32_t size_;
};

}  // namespace huadb
