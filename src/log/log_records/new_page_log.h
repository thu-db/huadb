#pragma once

#include "log/log_record.h"

namespace huadb {

class NewPageLog : public LogRecord {
 public:
  NewPageLog(xid_t xid, lsn_t prev_lsn, oid_t oid, pageid_t prev_page_id, pageid_t page_id);

  size_t SerializeTo(char *data) const override;
  static std::shared_ptr<NewPageLog> DeserializeFrom(const char *data);

  void Undo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager, lsn_t lsn,
            lsn_t undo_next_lsn) override;
  void Redo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager, lsn_t lsn) override;

  oid_t GetOid() const;
  pageid_t GetPageId() const;
  pageid_t GetPrevPageId() const;

 private:
  oid_t oid_;
  pageid_t prev_page_id_;
  pageid_t page_id_;
};

}  // namespace huadb
