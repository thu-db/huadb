#pragma once

#include <unordered_map>

#include "common/typedefs.h"
#include "log/log_record.h"

namespace huadb {

class EndCheckpointLog : public LogRecord {
 public:
  EndCheckpointLog(xid_t xid, lsn_t prev_lsn, const std::unordered_map<xid_t, lsn_t> &att,
                   const std::unordered_map<TablePageid, lsn_t> &dpt);

  size_t SerializeTo(char *data) const override;
  static std::shared_ptr<EndCheckpointLog> DeserializeFrom(const char *data);

  const std::unordered_map<xid_t, lsn_t> &GetATT();
  const std::unordered_map<TablePageid, lsn_t> &GetDPT();

 private:
  std::unordered_map<xid_t, lsn_t> att_;
  std::unordered_map<TablePageid, lsn_t> dpt_;
};

}  // namespace huadb
