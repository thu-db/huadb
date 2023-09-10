#include "transaction/transaction_manager.h"

#include <string>

#include "common/exceptions.h"

namespace huadb {

TransactionManager::TransactionManager(LockManager &lock_manager, xid_t next_xid)
    : lock_manager_(lock_manager), next_xid_(next_xid) {}

cid_t TransactionManager::GetCidAndIncrement(xid_t xid) {
  if (xid2cid_.find(xid) == xid2cid_.end()) {
    throw DbException("xid" + std::to_string(xid) + "not found in GetCidAndIncrement");
  }
  return xid2cid_[xid]++;
}

xid_t TransactionManager::GetNextXid() { return next_xid_; }

void TransactionManager::SetNextXid(xid_t next_xid) {
  if (next_xid > next_xid_) {
    next_xid_ = next_xid;
  }
}

xid_t TransactionManager::Begin() {
  auto xid = next_xid_++;
  std::unordered_set<xid_t> active_xids;
  for (const auto &entry : xid2cid_) {
    active_xids.insert(entry.first);
  }
  xid2active_set_[xid] = active_xids;
  xid2cid_[xid] = FIRST_CID;
  return xid;
}

void TransactionManager::Commit(xid_t xid) {
  if (xid2cid_.find(xid) == xid2cid_.end()) {
    throw DbException("xid" + std::to_string(xid) + "not found in xid2cid_ in Commit");
  }
  if (xid2active_set_.find(xid) == xid2active_set_.end()) {
    throw DbException("xid" + std::to_string(xid) + "not found in xid2active_set_ in Commit");
  }
  ReleaseLocks(xid);
  xid2cid_.erase(xid);
  xid2active_set_.erase(xid);
}

void TransactionManager::Rollback(xid_t xid) {
  if (xid2cid_.find(xid) == xid2cid_.end()) {
    throw DbException("xid" + std::to_string(xid) + "not found in xid2cid_ in Rollback");
  }
  if (xid2active_set_.find(xid) == xid2active_set_.end()) {
    throw DbException("xid" + std::to_string(xid) + "not found in xid2active_set_ in Rollback");
  }
  ReleaseLocks(xid);
  xid2cid_.erase(xid);
  xid2active_set_.erase(xid);
}

std::unordered_set<xid_t> TransactionManager::GetSnapshot(xid_t xid) { return xid2active_set_[xid]; }

std::unordered_set<xid_t> TransactionManager::GetActiveTransactions() {
  std::unordered_set<xid_t> active_xids;
  for (const auto &entry : xid2cid_) {
    active_xids.insert(entry.first);
  }
  return active_xids;
}

void TransactionManager::ReleaseLocks(xid_t xid) { lock_manager_.ReleaseLocks(xid); }

}  // namespace huadb
