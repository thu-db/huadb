#pragma once

#include "log/log_record.h"

namespace huadb {

class CommitLog : public LogRecord {
 public:
  CommitLog(xid_t xid, lsn_t prev_lsn);

  size_t SerializeTo(char *data) const override;
  static std::shared_ptr<CommitLog> DeserializeFrom(const char *data);
};

}  // namespace huadb
