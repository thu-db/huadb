#include "log/log_records/end_checkpoint_log.h"

#include <sstream>

namespace huadb {

EndCheckpointLog::EndCheckpointLog(lsn_t lsn, xid_t xid, lsn_t prev_lsn, const std::unordered_map<xid_t, lsn_t> &att,
                                   const std::unordered_map<TablePageid, lsn_t> &dpt)
    : LogRecord(LogType::END_CHECKPOINT, lsn, xid, prev_lsn), att_(att), dpt_(dpt) {
  size_ += sizeof(size_t) + att_.size() * (sizeof(xid_t) + sizeof(lsn_t)) + sizeof(size_t) +
           dpt_.size() * (sizeof(TablePageid) + sizeof(lsn_t));
}

size_t EndCheckpointLog::SerializeTo(char *data) const {
  size_t offset = LogRecord::SerializeTo(data);
  size_t att_size = att_.size();
  memcpy(data + offset, &att_size, sizeof(att_size));
  offset += sizeof(att_size);
  for (const auto [xid, lsn] : att_) {
    memcpy(data + offset, &xid, sizeof(xid_t));
    offset += sizeof(xid_t);
    memcpy(data + offset, &lsn, sizeof(lsn_t));
    offset += sizeof(lsn_t);
  }
  size_t dpt_size = dpt_.size();
  memcpy(data + offset, &dpt_size, sizeof(dpt_size));
  offset += sizeof(dpt_size);
  for (const auto [table_page_id, lsn] : dpt_) {
    memcpy(data + offset, &table_page_id, sizeof(TablePageid));
    offset += sizeof(TablePageid);
    memcpy(data + offset, &lsn, sizeof(lsn_t));
    offset += sizeof(lsn_t);
  }
  assert(offset == size_);
  return offset;
}

std::shared_ptr<EndCheckpointLog> EndCheckpointLog::DeserializeFrom(lsn_t lsn, const char *data) {
  xid_t xid;
  lsn_t prev_lsn;
  size_t offset = 0;
  memcpy(&xid, data + offset, sizeof(xid));
  offset += sizeof(xid);
  memcpy(&prev_lsn, data + offset, sizeof(prev_lsn));
  offset += sizeof(prev_lsn);
  size_t att_size;
  memcpy(&att_size, data + offset, sizeof(att_size));
  offset += sizeof(att_size);
  std::unordered_map<xid_t, lsn_t> att;
  xid_t temp_xid;
  lsn_t temp_lsn;
  for (size_t i = 0; i < att_size; i++) {
    memcpy(&temp_xid, data + offset, sizeof(temp_xid));
    offset += sizeof(temp_xid);
    memcpy(&temp_lsn, data + offset, sizeof(temp_lsn));
    offset += sizeof(temp_lsn);
    att[temp_xid] = temp_lsn;
  }
  size_t dpt_size;
  memcpy(&dpt_size, data + offset, sizeof(dpt_size));
  offset += sizeof(dpt_size);
  std::unordered_map<TablePageid, lsn_t> dpt;
  TablePageid unique_page_id;
  for (size_t i = 0; i < dpt_size; i++) {
    memcpy(&unique_page_id, data + offset, sizeof(TablePageid));
    offset += sizeof(TablePageid);
    memcpy(&temp_lsn, data + offset, sizeof(temp_lsn));
    offset += sizeof(temp_lsn);
    dpt[unique_page_id] = temp_lsn;
  }
  return std::make_shared<EndCheckpointLog>(lsn, xid, prev_lsn, std::move(att), std::move(dpt));
}

const std::unordered_map<xid_t, lsn_t> &EndCheckpointLog::GetATT() const { return att_; }

const std::unordered_map<TablePageid, lsn_t> &EndCheckpointLog::GetDPT() const { return dpt_; }

std::string EndCheckpointLog::ToString() const {
  std::ostringstream oss;
  oss << "EndCheckpointLog\t[" << LogRecord::ToString() << " att: {";
  for (const auto [xid, lsn] : att_) {
    oss << "(" << xid << ": " << lsn << ") ";
  }
  oss << "} dpt: {";
  for (const auto [table_page_id, lsn] : dpt_) {
    oss << "(" << table_page_id.table_oid_ << ", " << table_page_id.page_id_ << ": " << lsn << ") ";
  }
  oss << "}]";
  return oss.str();
}

}  // namespace huadb
