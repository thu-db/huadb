#pragma once

#include "log/log_record.h"

namespace huadb {

class BeginCheckpointLog : public LogRecord {
 public:
  BeginCheckpointLog(xid_t xid, lsn_t prev_lsn);

  size_t SerializeTo(char *data) const override;
  static std::shared_ptr<BeginCheckpointLog> DeserializeFrom(const char *data);
};

}  // namespace huadb
