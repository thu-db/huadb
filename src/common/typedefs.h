#pragma once

#include <cstdint>
#include <functional>

namespace huadb {

typedef uint64_t lsn_t;
typedef uint32_t xid_t;
typedef uint32_t cid_t;
typedef uint32_t oid_t;
typedef uint32_t pageid_t;
typedef uint16_t slotid_t;
typedef uint16_t db_size_t;

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
