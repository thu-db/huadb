#pragma once

#include "log/log_record.h"

namespace huadb {

class RollbackLog : public LogRecord {
 public:
  RollbackLog(xid_t xid, lsn_t prev_lsn);

  size_t SerializeTo(char *data) const override;
  static std::shared_ptr<RollbackLog> DeserializeFrom(const char *data);
};

}  // namespace huadb
