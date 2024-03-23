#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column_definition.h"
#include "common/types.h"
#include "log/log_manager.h"
#include "optimizer/optimizer.h"
#include "planner/planner.h"
#include "storage/buffer_pool.h"
#include "storage/disk.h"
#include "transaction/lock_manager.h"
#include "transaction/transaction_manager.h"

namespace huadb {

class Connection;
class ResultWriter;
class ExplainStatement;
class LockStatement;
class VariableSetStatement;
class VariableShowStatement;
class AnalyzeStatement;
class VacuumStatement;

class DatabaseEngine {
 public:
  DatabaseEngine();
  ~DatabaseEngine();

  const std::string &GetCurrentDatabase() const;
  bool InTransaction(const Connection &connection) const;
  void ExecuteSql(const std::string &sql, ResultWriter &writer, const Connection &connection);

  void Crash();
  void Flush();

  void Rollback(const Connection &connection);

 private:
  void Help(ResultWriter &writer) const;

  // 检查事务是否正在进行（不包含自动开启的事务）
  bool CheckInTransaction(const Connection &connection) const;

  void CreateDatabase(const std::string &db_name, bool exists_ok, ResultWriter &writer);
  void ShowDatabases(ResultWriter &writer) const;
  void ChangeDatabase(const std::string &db_name, ResultWriter &writer);
  void DropDatabase(const std::string &db_name, bool missing_ok, ResultWriter &writer);
  void CloseDatabase();

  void CreateTable(const std::string &table_name, const ColumnList &column_list, ResultWriter &writer);
  void DescribeTable(const std::string &table_name, ResultWriter &writer) const;
  void ShowTables(ResultWriter &writer) const;
  void DropTable(const std::string &table_name, ResultWriter &writer);

  void CreateIndex(const std::string &index_name, const std::string &table_name,
                   const std::vector<std::string> &column_names, ResultWriter &writer);
  void DropIndex(const std::string &index_name, ResultWriter &writer);

  void Begin(const Connection &connection);
  void Commit(const Connection &connection);

  void Checkpoint();
  void Recover();

  void Explain(const ExplainStatement &stmt, ResultWriter &writer);
  void Lock(xid_t xid, const LockStatement &stmt, ResultWriter &writer);

  void VariableSet(const Connection &connection, const VariableSetStatement &stmt, ResultWriter &writer);
  void VariableShow(const Connection &connection, const VariableShowStatement &stmt, ResultWriter &writer) const;

  void Analyze(const AnalyzeStatement &stmt, ResultWriter &writer);
  void Vacuum(const VacuumStatement &stmt, ResultWriter &writer);

  void WriteOneCell(const std::string &str, ResultWriter &writer) const;

  static IsolationLevel String2IsolationLevel(const std::string &str);
  static ForceJoin String2ForceJoin(const std::string &str);
  static JoinOrderAlgorithm String2JoinOrderAlgorithm(const std::string &str);
  static DeadlockType String2DeadlockType(const std::string &str);
  static bool String2Bool(const std::string &str);

  std::string current_db_;

  std::shared_ptr<Catalog> catalog_;
  std::shared_ptr<BufferPool> buffer_pool_;
  std::unique_ptr<Disk> disk_;
  std::unique_ptr<TransactionManager> transaction_manager_;
  std::unique_ptr<LogManager> log_manager_;
  std::unique_ptr<LockManager> lock_manager_;

  std::unordered_map<const Connection *, std::unordered_map<std::string, std::string>> client_variables_;
  std::unordered_map<const Connection *, xid_t> xids_;
  std::unordered_map<const Connection *, IsolationLevel> isolation_levels_;
  std::unordered_set<const Connection *> auto_transaction_set_;

  ForceJoin force_join_ = ForceJoin::NONE;
  JoinOrderAlgorithm join_order_algorithm_ = DEFAULT_JOIN_ORDER_ALGORITHM;
  bool enable_optimizer_ = true;
  bool enable_projection_pushdown_ = false;

  bool crashed_ = false;
};

}  // namespace huadb

// DBTRAIN_SYSTEMMANAGER_H
