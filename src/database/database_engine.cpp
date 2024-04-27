#include "database/database_engine.h"

#include <exception>

#include "binder/binder.h"
#include "binder/statements/statements.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/result_writer.h"
#include "common/string_util.h"
#include "database/connection.h"
#include "executors/executor_context.h"
#include "executors/executor_factory.h"
#include "operators/expressions/column_value.h"
#include "postgres_parser.hpp"
#include "table/record.h"

namespace huadb {

DatabaseEngine::DatabaseEngine() {
  // 数据库是否正常关闭
  bool normal_shutdown = true;
  disk_ = std::make_unique<Disk>();
  lock_manager_ = std::make_unique<LockManager>();
  oid_t oid = PRESERVED_OID;
  // 如存在控制文件，读取文件内容
  if (disk_->FileExists(CONTROL_NAME)) {
    std::ifstream in(CONTROL_NAME);
    xid_t xid;
    lsn_t lsn;
    // 下一个事务id，lsn，oid，以及是否正常关闭
    in >> xid >> lsn >> oid >> normal_shutdown;
    std::ofstream out(CONTROL_NAME);
    out.flush();
    out << xid << " " << lsn << " " << oid << " " << false << std::endl;
    transaction_manager_ = std::make_unique<TransactionManager>(*lock_manager_, xid);
    log_manager_ = std::make_unique<LogManager>(*disk_, *transaction_manager_, lsn);
  } else {
    std::ofstream out(CONTROL_NAME);
    out << FIRST_XID << " " << FIRST_LSN << " " << PRESERVED_OID << " " << false;
    transaction_manager_ = std::make_unique<TransactionManager>(*lock_manager_, FIRST_XID);
    log_manager_ = std::make_unique<LogManager>(*disk_, *transaction_manager_, FIRST_LSN);
  }
  buffer_pool_ = std::make_shared<BufferPool>(*disk_, *log_manager_);
  log_manager_->SetBufferPool(buffer_pool_);

  catalog_ = std::make_unique<Catalog>(*buffer_pool_, *log_manager_, oid);
  log_manager_->SetCatalog(catalog_);

  // 如果不存在 init 文件，创建系统表；如存在，则载入系统表
  if (!disk_->FileExists(INIT_NAME)) {
    catalog_->CreateSystemTables();
    disk_->CreateFile(INIT_NAME);
  } else {
    catalog_->LoadSystemTables();
  }
  current_db_ = DEFAULT_DATABASE_NAME;

  if (!normal_shutdown) {
    Recover();
  }
}

DatabaseEngine::~DatabaseEngine() {
  // 如果数据库不是崩溃状态，关闭数据库
  if (std::uncaught_exceptions() == 0 && !crashed_) {
    CloseDatabase();
  }
}

const std::string &DatabaseEngine::GetCurrentDatabase() const { return current_db_; }

bool DatabaseEngine::InTransaction(const Connection &connection) const {
  return xids_.find(&connection) != xids_.end();
}

void DatabaseEngine::ExecuteSql(const std::string &sql, ResultWriter &writer, const Connection &connection) {
  if (!sql.empty() && sql[0] == '\\') {
    if (sql[1] == 'l') {
      ShowDatabases(writer);
    } else if (sql[1] == 'd') {
      if (sql.size() <= 3) {
        ShowTables(writer);
      } else {
        auto table_name = sql.substr(3);
        StringUtil::RTrim(table_name);
        DescribeTable(table_name, writer);
      }
    } else if (sql[1] == 'c') {
      if (InTransaction(connection)) {
        throw DbException("Cannot execute \\c within a transaction block");
      }
      if (sql.size() > 3) {
        auto db_name = sql.substr(3);
        StringUtil::RTrim(db_name);
        ChangeDatabase(db_name, writer);
      } else {
        throw DbException("No database name");
      }
    } else if (sql[1] == '?' || sql[1] == 'h') {
      Help(writer);
    } else {
      throw DbException("Unknown command " + sql + "\nType \\? or \\h for help.");
    }
    return;
  }

  // 使用 PostgresParser 解析 SQL
  duckdb::PostgresParser parser;
  parser.Parse(sql);
  if (!parser.success) {
    throw DbException(parser.error_message);
  }
  if (parser.parse_tree == nullptr) {
    return;
  }

  // 将解析得到的语法树转换为语句节点
  std::vector<duckdb_libpgquery::PGNode *> statement_nodes;
  for (auto *node = parser.parse_tree->head; node != nullptr; node = lnext(node)) {
    statement_nodes.push_back(reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value));
  }

  // Binder 负责语义分析，如检查查询涉及的表是否存在，如存在则绑定表的元数据
  Binder binder(*catalog_);
  for (auto *stmt : statement_nodes) {
    auto statement = binder.BindStatement(stmt);
    if (statement->type_ == StatementType::TRANSACTION_STATEMENT) {
      const auto &transaction_statement = dynamic_cast<TransactionStatement &>(*statement);
      switch (transaction_statement.type_) {
        case TransactionType::BEGIN:
          Begin(connection);
          WriteOneCell("BEGIN", writer);
          break;
        case TransactionType::COMMIT:
          Commit(connection);
          WriteOneCell("COMMIT", writer);
          break;
        case TransactionType::ROLLBACK:
          Rollback(connection);
          WriteOneCell("ROLLBACK", writer);
          break;
      }
      continue;
    }
    if (statement->type_ == StatementType::CHECKPOINT_STATEMENT) {
      Checkpoint();
      WriteOneCell("CHECKPOINT", writer);
      continue;
    }

    bool is_modification_sql = false;
    // 如果该语句不在事务块内，则自动开启一个事务
    if (!InTransaction(connection)) {
      Begin(connection);
      auto_transaction_set_.insert(&connection);
    }
    try {
      switch (statement->type_) {
        // 对于 DDL 查询，直接调用对应的函数
        case StatementType::CREATE_DATABASE_STATEMENT: {
          if (CheckInTransaction(connection)) {
            throw DbException("Cannot execute DDL statement within a transaction block");
          }
          const auto &create_database_statement = dynamic_cast<CreateDatabaseStatement &>(*statement);
          CreateDatabase(create_database_statement.database_, false, writer);
          break;
        }
        case StatementType::CREATE_TABLE_STATEMENT: {
          if (CheckInTransaction(connection)) {
            throw DbException("Cannot execute DDL statement within a transaction block");
          }
          const auto &create_table_statement = dynamic_cast<CreateTableStatement &>(*statement);
          CreateTable(create_table_statement.table_, ColumnList(create_table_statement.columns_), writer);
          break;
        }
        case StatementType::CREATE_INDEX_STATEMENT: {
          if (CheckInTransaction(connection)) {
            throw DbException("Cannot execute DDL statement within a transaction block");
          }
          const auto &create_index_statement = dynamic_cast<CreateIndexStatement &>(*statement);
          CreateIndex(create_index_statement.index_name_, create_index_statement.table_name_,
                      create_index_statement.column_names_, writer);
          break;
        }
        case StatementType::DROP_DATABASE_STATEMENT: {
          if (CheckInTransaction(connection)) {
            throw DbException("Cannot execute DDL statement within a transaction block");
          }
          const auto &drop_database_statement = dynamic_cast<DropDatabaseStatement &>(*statement);
          DropDatabase(drop_database_statement.database_, drop_database_statement.missing_ok_, writer);
          break;
        }
        case StatementType::DROP_TABLE_STATEMENT: {
          if (CheckInTransaction(connection)) {
            throw DbException("Cannot execute DDL statement within a transaction block");
          }
          const auto &drop_table_statement = dynamic_cast<DropTableStatement &>(*statement);
          DropTable(drop_table_statement.table_, writer);
          break;
        }
        case StatementType::DROP_INDEX_STATEMENT: {
          if (CheckInTransaction(connection)) {
            throw DbException("Cannot execute DDL statement within a transaction block");
          }
          const auto &drop_index_statement = dynamic_cast<DropIndexStatement &>(*statement);
          DropIndex(drop_index_statement.index_name_, writer);
          break;
        }
        case StatementType::EXPLAIN_STATEMENT: {
          const auto &explain_statement = dynamic_cast<ExplainStatement &>(*statement);
          Explain(explain_statement, writer);
          break;
        }
        case StatementType::LOCK_STATEMENT: {
          const auto &lock_statement = dynamic_cast<LockStatement &>(*statement);
          Lock(xids_[&connection], lock_statement, writer);
          break;
        }
        case StatementType::VARIABLE_SET_STATEMENT: {
          const auto &variable_set_statement = dynamic_cast<VariableSetStatement &>(*statement);
          VariableSet(connection, variable_set_statement, writer);
          break;
        }
        case StatementType::VARIABLE_SHOW_STATEMENT: {
          const auto &variable_show_statement = dynamic_cast<VariableShowStatement &>(*statement);
          VariableShow(connection, variable_show_statement, writer);
          break;
        }
        case StatementType::ANALYZE_STATEMENT: {
          const auto &analyze_statement = dynamic_cast<AnalyzeStatement &>(*statement);
          Analyze(analyze_statement, writer);
          break;
        }
        case StatementType::VACUUM_STATEMENT: {
          const auto &vacuum_statement = dynamic_cast<VacuumStatement &>(*statement);
          Vacuum(vacuum_statement, writer);
          break;
        }
        case StatementType::UPDATE_STATEMENT:
        case StatementType::DELETE_STATEMENT:
          is_modification_sql = true;
        // 对于 DML 查询，需要生成查询计划并执行
        default: {
          try {
            // 生成查询计划
            Planner planner(force_join_);
            auto plan = planner.PlanQuery(*statement);

            if (enable_optimizer_) {
              // 查询计划优化
              Optimizer optimizer(*catalog_, join_order_algorithm_, enable_projection_pushdown_);
              plan = optimizer.Optimize(plan);
            }

            // 得到优化后的查询计划后，打印表头
            auto column_list = plan->OutputColumns();
            writer.BeginTable();
            writer.BeginHeader();
            for (size_t i = 0; i < column_list.Length(); i++) {
              writer.WriteHeaderCell(column_list.GetColumn(i).name_);
            }
            writer.EndHeader();

            // 生成查询上下文信息，如查询属于哪个事务，隔离级别等
            IsolationLevel isolation_level = DEFAULT_ISOLATION_LEVEL;
            if (isolation_levels_.find(&connection) != isolation_levels_.end()) {
              isolation_level = isolation_levels_[&connection];
            }
            auto executor_context = std::make_unique<ExecutorContext>(
                *buffer_pool_, *catalog_, *transaction_manager_, *lock_manager_, xids_[&connection], isolation_level,
                transaction_manager_->GetCidAndIncrement(xids_[&connection]), is_modification_sql);

            // 根据查询上下文和查询计划，生成执行器
            auto executor = ExecutorFactory::CreateExecutor(*executor_context, plan);
            executor->Init();
            size_t record_count = 0;
            while (auto record = executor->Next()) {
              writer.BeginRow();
              for (const auto &value : record->GetValues()) {
                writer.WriteCell(value.ToString());
              }
              writer.EndRow();
              record_count++;
            }
            writer.EndTable();
            writer.WriteRowCount(record_count);
          } catch (DbException &e) {
            if (auto_transaction_set_.find(&connection) != auto_transaction_set_.end()) {
              Rollback(connection);
              auto_transaction_set_.erase(&connection);
            }
            throw e;
          }
          break;
        }
      }
    } catch (DbException &e) {
      if (auto_transaction_set_.find(&connection) != auto_transaction_set_.end()) {
        Rollback(connection);
        auto_transaction_set_.erase(&connection);
      }
      throw e;
    }
    // 如果事务是自动开启的，查询结束后需要自动提交
    if (auto_transaction_set_.find(&connection) != auto_transaction_set_.end()) {
      Commit(connection);
      auto_transaction_set_.erase(&connection);
    }
  }
}

void DatabaseEngine::Crash() {
  buffer_pool_->Clear();
  log_manager_->Clear();
  crashed_ = true;
}

void DatabaseEngine::Flush() { buffer_pool_->Flush(); }

void DatabaseEngine::Help(ResultWriter &writer) const {
  std::string help = R"(
  \? or \h              show help message
  \c [database_name]    change database
  \d                    show tables
  \d [table_name]       describe table
  \l                    show databases
  \q                    quit
  )";
  WriteOneCell(help, writer);
}

bool DatabaseEngine::CheckInTransaction(const Connection &connection) const {
  return auto_transaction_set_.find(&connection) == auto_transaction_set_.end() && InTransaction(connection);
}

void DatabaseEngine::CreateDatabase(const std::string &db_name, bool exists_ok, ResultWriter &writer) {
  catalog_->CreateDatabase(db_name, exists_ok);
  WriteOneCell("CREATE DATABASE", writer);
}

void DatabaseEngine::ShowDatabases(ResultWriter &writer) const {
  writer.BeginTable();
  writer.BeginHeader();
  writer.WriteHeaderCell("database_name");
  writer.EndHeader();
  size_t db_count = 0;
  for (const auto &db_name : catalog_->GetDatabaseNames()) {
    writer.BeginRow();
    writer.WriteCell(db_name);
    writer.EndRow();
    db_count++;
  }
  writer.EndTable();
  writer.WriteRowCount(db_count);
}

void DatabaseEngine::ChangeDatabase(const std::string &db_name, ResultWriter &writer) {
  if (db_name != current_db_) {
    catalog_->ChangeDatabase(db_name);
    current_db_ = db_name;
  }
  WriteOneCell("Change to database " + db_name, writer);
}

void DatabaseEngine::DropDatabase(const std::string &db_name, bool missing_ok, ResultWriter &writer) {
  if (db_name == current_db_) {
    throw DbException("Cannot drop the currently open database");
  }
  catalog_->DropDatabase(db_name, missing_ok);
  WriteOneCell("DROP DATABASE", writer);
}

void DatabaseEngine::CloseDatabase() {
  buffer_pool_->Flush();
  log_manager_->Flush();
  log_manager_->Checkpoint();

  std::ofstream control(CONTROL_NAME);
  control << transaction_manager_->GetNextXid() << " " << log_manager_->GetNextLSN() << " " << catalog_->GetNextOid()
          << " " << true;
}

void DatabaseEngine::CreateTable(const std::string &table_name, const ColumnList &column_list, ResultWriter &writer) {
  catalog_->CreateTable(table_name, column_list);
  WriteOneCell("CREATE TABLE", writer);
}

void DatabaseEngine::DescribeTable(const std::string &table_name, ResultWriter &writer) const {
  auto column_list = catalog_->GetTableColumnList(table_name);
  writer.BeginTable();
  writer.BeginHeader();
  writer.WriteHeaderCell("name");
  writer.WriteHeaderCell("type");
  writer.WriteHeaderCell("size");
  writer.EndHeader();
  size_t column_count = 0;
  for (const auto &column : column_list.GetColumns()) {
    writer.BeginRow();
    for (const auto &value : column.ToStringVector()) {
      writer.WriteCell(value);
    }
    writer.EndRow();
    column_count++;
  }
  writer.EndTable();
  writer.WriteRowCount(column_count);
}

void DatabaseEngine::ShowTables(ResultWriter &writer) const {
  writer.BeginTable();
  writer.BeginHeader();
  writer.WriteHeaderCell("table_name");
  writer.EndHeader();
  size_t table_count = 0;
  for (const auto &table_name : catalog_->GetTableNames()) {
    writer.BeginRow();
    writer.WriteCell(table_name);
    writer.EndRow();
    table_count++;
  }
  writer.EndTable();
  writer.WriteRowCount(table_count);
}

void DatabaseEngine::DropTable(const std::string &table_name, ResultWriter &writer) {
  catalog_->DropTable(table_name);
  WriteOneCell("DROP TABLE", writer);
}

void DatabaseEngine::CreateIndex(const std::string &index_name, const std::string &table_name,
                                 const std::vector<std::string> &column_names, ResultWriter &writer) {
  // Not implemented yet
  WriteOneCell("CREATE INDEX", writer);
}

void DatabaseEngine::DropIndex(const std::string &index_name, ResultWriter &writer) {
  // Not implemented yet
  WriteOneCell("DROP INDEX", writer);
}

void DatabaseEngine::Begin(const Connection &connection) {
  if (InTransaction(connection)) {
    throw DbException("There is already a transaction in progress");
  } else {
    auto xid = transaction_manager_->Begin();
    xids_[&connection] = xid;
    log_manager_->AppendBeginLog(xid);
  }
}

void DatabaseEngine::Commit(const Connection &connection) {
  if (!InTransaction(connection)) {
    throw DbException("There is no transaction in process");
  } else {
    log_manager_->AppendCommitLog(xids_[&connection]);
    transaction_manager_->Commit(xids_[&connection]);
    xids_.erase(&connection);
  }
}

void DatabaseEngine::Rollback(const Connection &connection) {
  if (!InTransaction(connection)) {
    throw DbException("There is no transaction in process");
  } else {
    log_manager_->Rollback(xids_[&connection]);
    log_manager_->AppendRollbackLog(xids_[&connection]);
    transaction_manager_->Rollback(xids_[&connection]);
    xids_.erase(&connection);
  }
}

void DatabaseEngine::Checkpoint() { log_manager_->Checkpoint(); }

void DatabaseEngine::Recover() { log_manager_->Recover(); }

void DatabaseEngine::Lock(xid_t xid, const LockStatement &stmt, ResultWriter &writer) {
  LockType lock_type;
  if (stmt.lock_type_ == TableLockType::SHARE) {
    lock_type = LockType::S;
  } else if (stmt.lock_type_ == TableLockType::EXCLUSIVE) {
    lock_type = LockType::X;
  } else {
    throw DbException("Unknown lock type");
  }
  if (!lock_manager_->LockTable(xid, lock_type, stmt.table_->oid_)) {
    throw DbException("Cannot acquire lock");
  }
}

void DatabaseEngine::Explain(const ExplainStatement &stmt, ResultWriter &writer) {
  std::string output;
  if ((stmt.options_ & ExplainOptions::BINDER) != 0) {
    output += "===Binder===\n";
    output += stmt.statement_->ToString();
  }

  Planner planner(force_join_);
  auto plan = planner.PlanQuery(*stmt.statement_);
  if ((stmt.options_ & ExplainOptions::PLANNER) != 0) {
    output += "===Planner===\n";
    output += plan->ToString();
    output += "\n";
  }

  if (enable_optimizer_) {
    Optimizer optimizer(*catalog_, join_order_algorithm_, enable_projection_pushdown_);
    plan = optimizer.Optimize(plan);
  }

  if ((stmt.options_ & ExplainOptions::OPTIMIZER) != 0) {
    output += "===Optimizer===\n";
    output += plan->ToString();
  }

  WriteOneCell(output, writer);
}

void DatabaseEngine::VariableSet(const Connection &connection, const VariableSetStatement &stmt, ResultWriter &writer) {
  if (stmt.variable_ == "isolation_level") {
    isolation_levels_[&connection] = String2IsolationLevel(stmt.value_);
  } else if (stmt.variable_ == "join_order_algorithm") {
    join_order_algorithm_ = String2JoinOrderAlgorithm(stmt.value_);
  } else if (stmt.variable_ == "force_join") {
    force_join_ = String2ForceJoin(stmt.value_);
  } else if (stmt.variable_ == "enable_optimizer") {
    enable_optimizer_ = String2Bool(stmt.value_);
  } else if (stmt.variable_ == "enable_projection_pushdown") {
    enable_projection_pushdown_ = String2Bool(stmt.value_);
  } else if (stmt.variable_ == "deadlock") {
    lock_manager_->SetDeadLockType(String2DeadlockType(stmt.value_));
  }
  client_variables_[&connection][stmt.variable_] = stmt.value_;
  WriteOneCell("SET", writer);
}

void DatabaseEngine::VariableShow(const Connection &connection, const VariableShowStatement &stmt,
                                  ResultWriter &writer) const {
  std::string result;
  if (stmt.variable_ == "tables") {
    ShowTables(writer);
    return;
  } else if (stmt.variable_ == "databases") {
    ShowDatabases(writer);
    return;
  } else if (stmt.variable_ == "disk_access_count") {
    result = std::to_string(disk_->GetAccessCount());
  } else if (stmt.variable_ == "redo_count") {
    result = std::to_string(log_manager_->GetRedoCount());
  } else {
    if (client_variables_.find(&connection) == client_variables_.end() ||
        client_variables_.at(&connection).find(stmt.variable_) == client_variables_.at(&connection).end()) {
      throw DbException("Variable not found");
    }
    result = client_variables_.at(&connection).at(stmt.variable_);
  }
  writer.BeginTable();
  writer.BeginHeader();
  writer.WriteHeaderCell(stmt.variable_);
  writer.EndHeader();
  writer.BeginRow();
  writer.WriteCell(result);
  writer.EndRow();
  writer.EndTable();
  writer.WriteRowCount(1);
}

void DatabaseEngine::Analyze(const AnalyzeStatement &stmt, ResultWriter &writer) {
  std::vector<std::string> table_names;
  std::vector<uint32_t> column_idxs;
  std::vector<ColumnValue> columns;
  if (stmt.table_ == nullptr) {
    table_names = catalog_->GetTableNames();
  } else {
    table_names.push_back(stmt.table_->table_);
    if (!stmt.columns_.empty()) {
      for (const auto &column : stmt.columns_) {
        auto column_list = catalog_->GetTableColumnList(stmt.table_->table_);
        auto col_idx = column_list.GetColumnIndex(column->col_name_[1]);
        auto col_type = column_list.GetColumn(col_idx).type_;
        auto col_name = column_list.GetColumn(col_idx).name_;
        auto col_size = column_list.GetColumn(col_idx).GetMaxSize();
        columns.emplace_back(col_idx, col_type, col_name, col_size, true);
      }
    }
  }
  for (const auto &table_name : table_names) {
    auto oid = catalog_->GetTableOid(table_name);
    auto table = catalog_->GetTable(oid);
    if (stmt.columns_.empty()) {
      auto column_list = catalog_->GetTableColumnList(table_name);
      columns.clear();
      for (size_t i = 0; i < column_list.Length(); i++) {
        auto col_type = column_list.GetColumn(i).type_;
        auto col_name = column_list.GetColumn(i).name_;
        auto col_size = column_list.GetColumn(i).GetMaxSize();
        columns.emplace_back(i, col_type, col_name, col_size, true);
      }
    }
    auto scan = std::make_unique<TableScan>(*buffer_pool_, table, Rid{table->GetFirstPageId(), 0});
    uint32_t record_count = 0;
    std::vector<std::unordered_set<Value>> value_set;
    value_set.resize(columns.size());
    while (auto record = scan->GetNextRecord()) {
      for (size_t i = 0; i < columns.size(); i++) {
        value_set[i].insert(record->GetValue(columns[i].GetColumnIndex()));
      }
      record_count++;
    }
    catalog_->SetCardinality(table_name, record_count);
    for (size_t i = 0; i < columns.size(); i++) {
      catalog_->SetDistinct(table_name, columns[i].name_, value_set[i].size());
    }
  }
  WriteOneCell("Analyze", writer);
}

void DatabaseEngine::Vacuum(const VacuumStatement &stmt, ResultWriter &writer) {
  // LAB 1 ADVANCED BEGIN
  WriteOneCell("Vacuum", writer);
}

void DatabaseEngine::WriteOneCell(const std::string &str, ResultWriter &writer) const {
  writer.BeginTable(true);
  writer.BeginRow();
  writer.WriteCell(str);
  writer.EndRow();
  writer.EndTable();
}

IsolationLevel DatabaseEngine::String2IsolationLevel(const std::string &str) {
  if (str == "read_committed") {
    return IsolationLevel::READ_COMMITTED;
  } else if (str == "repeatable_read") {
    return IsolationLevel::REPEATABLE_READ;
  } else if (str == "serializable") {
    return IsolationLevel::SERIALIZABLE;
  } else {
    throw DbException("Unknown isolation level " + str);
  }
}

ForceJoin DatabaseEngine::String2ForceJoin(const std::string &str) {
  if (str == "none") {
    return ForceJoin::NONE;
  } else if (str == "hash") {
    return ForceJoin::HASH;
  } else if (str == "merge") {
    return ForceJoin::MERGE;
  } else {
    throw DbException("Unknown force join " + str);
  }
}

JoinOrderAlgorithm DatabaseEngine::String2JoinOrderAlgorithm(const std::string &str) {
  if (str == "none") {
    return JoinOrderAlgorithm::NONE;
  } else if (str == "dp") {
    return JoinOrderAlgorithm::DP;
  } else if (str == "greedy") {
    return JoinOrderAlgorithm::GREEDY;
  } else {
    throw DbException("Unknown join order algorithm " + str);
  }
}

DeadlockType DatabaseEngine::String2DeadlockType(const std::string &str) {
  if (str == "wait_die") {
    return DeadlockType::WAIT_DIE;
  } else if (str == "wound_wait") {
    return DeadlockType::WOUND_WAIT;
  } else if (str == "detection") {
    return DeadlockType::DETECTION;
  } else {
    throw DbException("Unknown deadlock type " + str);
  }
}

bool DatabaseEngine::String2Bool(const std::string &str) {
  if (str == "true" || str == "1" || str == "on") {
    return true;
  } else if (str == "false" || str == "0" || str == "off") {
    return false;
  }
  throw DbException("Unknown boolean value " + str);
}

}  // namespace huadb
