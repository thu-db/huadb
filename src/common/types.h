#pragma once

#include <cstdint>
#include <functional>

namespace huadb {

using lsn_t = uint64_t;  // log sequence number
using xid_t = uint32_t;  // transaction id
using cid_t = uint32_t;  // commit id
using oid_t = uint32_t;  // object id
using pageid_t = uint32_t;
using slotid_t = uint16_t;
using db_size_t = uint16_t;
using enum_t = uint8_t;

struct Rid {
  pageid_t page_id_;
  slotid_t slot_id_;
};

struct TablePageid {
  oid_t table_oid_;
  pageid_t page_id_;
  bool operator==(const TablePageid &other) const {
    return (table_oid_ == other.table_oid_) && (page_id_ == other.page_id_);
  }
};

struct Slot {
  db_size_t offset_;
  db_size_t size_;
};

}  // namespace huadb

namespace std {

template <>
struct hash<huadb::TablePageid> {
  uint64_t operator()(const huadb::TablePageid &other) const {
    return (static_cast<uint64_t>(other.table_oid_) << 32) | other.page_id_;
  }
};

}  // namespace std
