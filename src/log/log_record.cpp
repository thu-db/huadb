#include "log/log_record.h"

#include "log/log_records/log_records.h"

namespace huadb {

LogRecord::LogRecord(LogType type, lsn_t lsn, xid_t xid, lsn_t prev_lsn)
    : type_(type), lsn_(lsn), xid_(xid), prev_lsn_(prev_lsn) {
  // LSN 为日志记录在日志文件中的位置，无需占用空间
  size_ = sizeof(type_) + sizeof(xid_) + sizeof(prev_lsn_);
}

size_t LogRecord::SerializeTo(char *data) const {
  size_t offset = 0;
  memcpy(data + offset, &type_, sizeof(type_));
  offset += sizeof(type_);
  memcpy(data + offset, &xid_, sizeof(xid_));
  offset += sizeof(xid_);
  memcpy(data + offset, &prev_lsn_, sizeof(prev_lsn_));
  offset += sizeof(prev_lsn_);
  return offset;
}

std::shared_ptr<LogRecord> LogRecord::DeserializeFrom(lsn_t lsn, const char *data) {
  LogType type;
  memcpy(&type, data, sizeof(type));
  switch (type) {
    case LogType::INSERT:
      return InsertLog::DeserializeFrom(lsn, data + sizeof(type));
    case LogType::DELETE:
      return DeleteLog::DeserializeFrom(lsn, data + sizeof(type));
    case LogType::NEW_PAGE:
      return NewPageLog::DeserializeFrom(lsn, data + sizeof(type));
    case LogType::BEGIN:
      return BeginLog::DeserializeFrom(lsn, data + sizeof(type));
    case LogType::COMMIT:
      return CommitLog::DeserializeFrom(lsn, data + sizeof(type));
    case LogType::ROLLBACK:
      return RollbackLog::DeserializeFrom(lsn, data + sizeof(type));
    case LogType::BEGIN_CHECKPOINT:
      return BeginCheckpointLog::DeserializeFrom(lsn, data + sizeof(type));
    case LogType::END_CHECKPOINT:
      return EndCheckpointLog::DeserializeFrom(lsn, data + sizeof(type));
    default:
      throw DbException("Unknown log type in DeserializeFrom");
  }
}

void LogRecord::Undo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager, lsn_t undo_next_lsn) {}

void LogRecord::Redo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager) {}

void LogRecord::SetLSN(lsn_t lsn) { lsn_ = lsn; }

LogType LogRecord::GetType() const { return type_; }

lsn_t LogRecord::GetLSN() const { return lsn_; }

xid_t LogRecord::GetXid() const { return xid_; }

lsn_t LogRecord::GetPrevLSN() const { return prev_lsn_; }

size_t LogRecord::GetSize() const { return size_; }

std::string LogRecord::ToString() const {
  return fmt::format("lsn: {}\tsize: {}\txid: {}\tprev_lsn: {}", lsn_, size_, xid_, prev_lsn_);
}

}  // namespace huadb
