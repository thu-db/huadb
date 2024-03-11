#pragma once

#include "log/log_record.h"

namespace huadb {

class BeginCheckpointLog : public LogRecord {
 public:
  BeginCheckpointLog(lsn_t lsn, xid_t xid, lsn_t prev_lsn);

  size_t SerializeTo(char *data) const override;
  static std::shared_ptr<BeginCheckpointLog> DeserializeFrom(lsn_t lsn, const char *data);

  std::string ToString() const override;
};

}  // namespace huadb
