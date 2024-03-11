#include "log/log_records/commit_log.h"

namespace huadb {

CommitLog::CommitLog(lsn_t lsn, xid_t xid, lsn_t prev_lsn) : LogRecord(LogType::COMMIT, lsn, xid, prev_lsn) {}

size_t CommitLog::SerializeTo(char *data) const {
  size_t offset = LogRecord::SerializeTo(data);
  assert(offset == size_);
  return offset;
}

std::shared_ptr<CommitLog> CommitLog::DeserializeFrom(lsn_t lsn, const char *data) {
  xid_t xid;
  lsn_t prev_lsn;
  size_t offset = 0;
  memcpy(&xid, data + offset, sizeof(xid));
  offset += sizeof(xid);
  memcpy(&prev_lsn, data + offset, sizeof(prev_lsn));
  offset += sizeof(prev_lsn);
  return std::make_shared<CommitLog>(lsn, xid, prev_lsn);
}

std::string CommitLog::ToString() const { return fmt::format("CommitLog\t\t[{}]", LogRecord::ToString()); }

}  // namespace huadb
