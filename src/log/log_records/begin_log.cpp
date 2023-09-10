#include "log/log_records/begin_log.h"

namespace huadb {

BeginLog::BeginLog(xid_t xid, lsn_t prev_lsn) : LogRecord(LogType::BEGIN, xid, prev_lsn) {}

size_t BeginLog::SerializeTo(char *data) const {
  size_t offset = LogRecord::SerializeTo(data);
  assert(offset == size_);
  return offset;
}

std::shared_ptr<BeginLog> BeginLog::DeserializeFrom(const char *data) {
  xid_t xid;
  lsn_t prev_lsn;
  size_t offset = 0;
  memcpy(&xid, data + offset, sizeof(xid));
  offset += sizeof(xid);
  memcpy(&prev_lsn, data + offset, sizeof(prev_lsn));
  offset += sizeof(prev_lsn);
  return std::make_shared<BeginLog>(xid, prev_lsn);
}

}  // namespace huadb
