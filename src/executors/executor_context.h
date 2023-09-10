#pragma once

#include "catalog/catalog.h"
#include "transaction/lock_manager.h"
#include "transaction/transaction_manager.h"

namespace huadb {

class ExecutorContext {
 public:
  ExecutorContext(BufferPool &buffer_pool, Catalog &catalog, TransactionManager &transaction_manager,
                  LockManager &lock_manager, xid_t xid, IsolationLevel isolation_level, cid_t cid,
                  bool is_modification_sql)
      : buffer_pool_(buffer_pool),
        catalog_(catalog),
        transaction_manager_(transaction_manager),
        lock_manager_(lock_manager),
        xid_(xid),
        isolation_level_(isolation_level),
        cid_(cid),
        is_modification_sql_(is_modification_sql) {}

  BufferPool &GetBufferPool() const { return buffer_pool_; }
  Catalog &GetCatalog() const { return catalog_; }
  TransactionManager &GetTransactionManager() const { return transaction_manager_; }
  LockManager &GetLockManager() const { return lock_manager_; }
  xid_t GetXid() const { return xid_; }
  IsolationLevel GetIsolationLevel() const { return isolation_level_; }
  cid_t GetCid() const { return cid_; }
  bool IsModificationSql() const { return is_modification_sql_; }

 private:
  BufferPool &buffer_pool_;
  Catalog &catalog_;
  TransactionManager &transaction_manager_;
  LockManager &lock_manager_;
  xid_t xid_;
  IsolationLevel isolation_level_;
  cid_t cid_;
  bool is_modification_sql_;
};

}  // namespace huadb
