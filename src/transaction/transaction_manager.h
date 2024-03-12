#pragma once

#include <atomic>
#include <unordered_map>
#include <unordered_set>

#include "common/constants.h"
#include "transaction/lock_manager.h"

namespace huadb {

enum class IsolationLevel {
  READ_COMMITTED,
  REPEATABLE_READ,
  SERIALIZABLE,
};
static constexpr IsolationLevel DEFAULT_ISOLATION_LEVEL = IsolationLevel::READ_COMMITTED;

class TransactionManager {
 public:
  explicit TransactionManager(LockManager &lock_manager, xid_t next_xid);

  // 获取 cid 并递增
  cid_t GetCidAndIncrement(xid_t xid);
  // 获取下一个 xid
  xid_t GetNextXid() const;
  // 设置 next_xid，用于故障恢复
  void SetNextXid(xid_t next_xid);

  xid_t Begin();
  void Commit(xid_t xid);
  void Rollback(xid_t xid);

  // 获取事务快照，即事务开始时的活跃事务表
  std::unordered_set<xid_t> GetSnapshot(xid_t xid) const;
  // 获取活跃事务表
  std::unordered_set<xid_t> GetActiveTransactions() const;

 private:
  // 释放事务持有的锁
  void ReleaseLocks(xid_t xid);

  LockManager &lock_manager_;
  std::atomic<xid_t> next_xid_ = 1;
  std::unordered_map<xid_t, cid_t> xid2cid_;
  std::unordered_map<xid_t, std::unordered_set<xid_t>> xid2active_set_;
};

}  // namespace huadb
