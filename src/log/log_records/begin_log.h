#pragma once

#include "log/log_record.h"

namespace huadb {

class BeginLog : public LogRecord {
 public:
  BeginLog(xid_t xid, lsn_t prev_lsn);

  size_t SerializeTo(char *data) const override;
  static std::shared_ptr<BeginLog> DeserializeFrom(const char *data);
};

}  // namespace huadb
