#include "log/log_records/begin_log.h"

namespace huadb {

BeginLog::BeginLog(lsn_t lsn, xid_t xid, lsn_t prev_lsn) : LogRecord(LogType::BEGIN, lsn, xid, prev_lsn) {}

size_t BeginLog::SerializeTo(char *data) const {
  size_t offset = LogRecord::SerializeTo(data);
  assert(offset == size_);
  return offset;
}

std::shared_ptr<BeginLog> BeginLog::DeserializeFrom(lsn_t lsn, const char *data) {
  xid_t xid;
  lsn_t prev_lsn;
  size_t offset = 0;
  memcpy(&xid, data + offset, sizeof(xid));
  offset += sizeof(xid);
  memcpy(&prev_lsn, data + offset, sizeof(prev_lsn));
  offset += sizeof(prev_lsn);
  return std::make_shared<BeginLog>(lsn, xid, prev_lsn);
}

std::string BeginLog::ToString() const { return fmt::format("BeginLog\t\t[{}]", LogRecord::ToString()); }

}  // namespace huadb
