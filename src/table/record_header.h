#pragma once

#include <string>

#include "common/constants.h"

namespace huadb {

// deleted(1) + xmin(4) + xmax(4) + cid(4) = 13
static constexpr db_size_t RECORD_HEADER_SIZE = sizeof(bool) + sizeof(xid_t) + sizeof(xid_t) + sizeof(cid_t);

class RecordHeader {
  friend class Record;

 public:
  db_size_t SerializeTo(char *data) const;
  db_size_t DeserializeFrom(const char *data);

  std::string ToString() const;

 private:
  // LAB 1: 记录是否删除
  bool deleted_ = false;

  // LAB 3: 记录的事务信息
  xid_t xmin_ = NULL_XID;
  xid_t xmax_ = NULL_XID;
  cid_t cid_ = NULL_CID;
};

}  // namespace huadb
