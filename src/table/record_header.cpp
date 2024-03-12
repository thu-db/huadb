#include "table/record_header.h"

#include <cassert>
#include <cstring>
#include <sstream>

namespace huadb {

db_size_t RecordHeader::SerializeTo(char *data) const {
  db_size_t offset = 0;
  memcpy(data + offset, &deleted_, sizeof(deleted_));
  offset += sizeof(deleted_);
  memcpy(data + offset, &xmin_, sizeof(xmin_));
  offset += sizeof(xmin_);
  memcpy(data + offset, &xmax_, sizeof(xmax_));
  offset += sizeof(xmax_);
  memcpy(data + offset, &cid_, sizeof(cid_));
  offset += sizeof(cid_);
  assert(offset == RECORD_HEADER_SIZE);
  return offset;
}

db_size_t RecordHeader::DeserializeFrom(const char *data) {
  db_size_t offset = 0;
  memcpy(&deleted_, data + offset, sizeof(deleted_));
  offset += sizeof(deleted_);
  memcpy(&xmin_, data + offset, sizeof(xmin_));
  offset += sizeof(xmin_);
  memcpy(&xmax_, data + offset, sizeof(xmax_));
  offset += sizeof(xmax_);
  memcpy(&cid_, data + offset, sizeof(cid_));
  offset += sizeof(cid_);
  assert(offset == RECORD_HEADER_SIZE);
  return offset;
}

std::string RecordHeader::ToString() const {
  std::ostringstream oss;
  oss << "[deleted: " << deleted_;
  oss << ", xmin: " << xmin_;
  oss << ", xmax: " << xmax_;
  oss << ", cid: " << cid_;
  oss << "]";
  return oss.str();
}

}  // namespace huadb
