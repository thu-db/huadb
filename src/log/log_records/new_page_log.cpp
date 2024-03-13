#include "log/log_records/new_page_log.h"

namespace huadb {

NewPageLog::NewPageLog(lsn_t lsn, xid_t xid, lsn_t prev_lsn, oid_t oid, pageid_t prev_page_id, pageid_t page_id)
    : LogRecord(LogType::NEW_PAGE, lsn, xid, prev_lsn), oid_(oid), prev_page_id_(prev_page_id), page_id_(page_id) {
  size_ += sizeof(oid_) + sizeof(page_id_) + sizeof(prev_page_id_);
}

size_t NewPageLog::SerializeTo(char *data) const {
  size_t offset = LogRecord::SerializeTo(data);
  memcpy(data + offset, &oid_, sizeof(oid_));
  offset += sizeof(oid_);
  memcpy(data + offset, &prev_page_id_, sizeof(prev_page_id_));
  offset += sizeof(prev_page_id_);
  memcpy(data + offset, &page_id_, sizeof(page_id_));
  offset += sizeof(page_id_);
  assert(offset == size_);
  return offset;
}

std::shared_ptr<NewPageLog> NewPageLog::DeserializeFrom(lsn_t lsn, const char *data) {
  xid_t xid;
  lsn_t prev_lsn;
  oid_t oid;
  pageid_t page_id, prev_page_id;
  size_t offset = 0;
  memcpy(&xid, data + offset, sizeof(xid));
  offset += sizeof(xid);
  memcpy(&prev_lsn, data + offset, sizeof(prev_lsn));
  offset += sizeof(prev_lsn);
  memcpy(&oid, data + offset, sizeof(oid));
  offset += sizeof(oid);
  memcpy(&prev_page_id, data + offset, sizeof(prev_page_id));
  offset += sizeof(prev_page_id);
  memcpy(&page_id, data + offset, sizeof(page_id));
  offset += sizeof(page_id);
  return std::make_shared<NewPageLog>(lsn, xid, prev_lsn, oid, prev_page_id, page_id);
}

void NewPageLog::Undo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager, lsn_t undo_next_lsn) {
  // LAB 2 BEGIN
}

void NewPageLog::Redo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager) {
  // 如果 oid_ 不存在，表示该表已经被删除，无需 redo
  if (!catalog.TableExists(oid_)) {
    return;
  }
  // 根据日志信息进行重做
  // LAB 2 BEGIN
}

oid_t NewPageLog::GetOid() const { return oid_; }

pageid_t NewPageLog::GetPageId() const { return page_id_; }

pageid_t NewPageLog::GetPrevPageId() const { return prev_page_id_; }

std::string NewPageLog::ToString() const {
  return fmt::format("NewPageLog\t\t[{}\toid: {}\tprev_page_id: {}\tpage_id: {}]", LogRecord::ToString(), oid_,
                     prev_page_id_, page_id_);
}

}  // namespace huadb
