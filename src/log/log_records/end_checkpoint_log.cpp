#include "log/log_records/end_checkpoint_log.h"

namespace huadb {

EndCheckpointLog::EndCheckpointLog(xid_t xid, lsn_t prev_lsn, const std::unordered_map<xid_t, lsn_t> &att,
                                   const std::unordered_map<TablePageid, lsn_t> &dpt)
    : LogRecord(LogType::END_CHECKPOINT, xid, prev_lsn), att_(att), dpt_(dpt) {
  size_ += sizeof(size_t) + att_.size() * (sizeof(xid_t) + sizeof(lsn_t)) + sizeof(size_t) +
           dpt_.size() * (sizeof(TablePageid) + sizeof(lsn_t));
}

size_t EndCheckpointLog::SerializeTo(char *data) const {
  size_t offset = LogRecord::SerializeTo(data);
  size_t att_size = att_.size();
  memcpy(data + offset, &att_size, sizeof(att_size));
  offset += sizeof(att_size);
  for (const auto &entry : att_) {
    memcpy(data + offset, &entry.first, sizeof(xid_t));
    offset += sizeof(xid_t);
    memcpy(data + offset, &entry.second, sizeof(lsn_t));
    offset += sizeof(lsn_t);
  }
  size_t dpt_size = dpt_.size();
  memcpy(data + offset, &dpt_size, sizeof(dpt_size));
  offset += sizeof(dpt_size);
  for (const auto &entry : dpt_) {
    memcpy(data + offset, &entry.first, sizeof(TablePageid));
    offset += sizeof(TablePageid);
    memcpy(data + offset, &entry.second, sizeof(lsn_t));
    offset += sizeof(lsn_t);
  }
  assert(offset == size_);
  return offset;
}

std::shared_ptr<EndCheckpointLog> EndCheckpointLog::DeserializeFrom(const char *data) {
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
  lsn_t lsn;
  for (size_t i = 0; i < att_size; i++) {
    memcpy(&xid, data + offset, sizeof(xid));
    offset += sizeof(xid);
    memcpy(&lsn, data + offset, sizeof(lsn));
    offset += sizeof(lsn);
    att[xid] = lsn;
  }
  size_t dpt_size;
  memcpy(&dpt_size, data + offset, sizeof(dpt_size));
  offset += sizeof(dpt_size);
  std::unordered_map<TablePageid, lsn_t> dpt;
  TablePageid unique_page_id;
  for (size_t i = 0; i < dpt_size; i++) {
    memcpy(&unique_page_id, data + offset, sizeof(TablePageid));
    offset += sizeof(TablePageid);
    memcpy(&lsn, data + offset, sizeof(lsn));
    offset += sizeof(lsn);
    dpt[unique_page_id] = lsn;
  }
  return std::make_shared<EndCheckpointLog>(xid, prev_lsn, std::move(att), std::move(dpt));
}

const std::unordered_map<xid_t, lsn_t> &EndCheckpointLog::GetATT() { return att_; }

const std::unordered_map<TablePageid, lsn_t> &EndCheckpointLog::GetDPT() { return dpt_; }

}  // namespace huadb
