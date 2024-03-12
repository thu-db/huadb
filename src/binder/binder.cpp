//===--------------------------------------------------------------------------------------------------------------===//
// Copyright 2018-2023 Stichting DuckDB Foundation

// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
// Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//===--------------------------------------------------------------------------------------------------------------===//

#include "binder/binder.h"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <unordered_set>

#include "binder/expressions/expressions.h"
#include "binder/order_by.h"
#include "binder/statements/statements.h"
#include "binder/table_refs/table_refs.h"
#include "catalog/column_definition.h"
#include "common/exceptions.h"
#include "common/value.h"
#include "nodes/parsenodes.hpp"

namespace huadb {

std::unique_ptr<Statement> Binder::BindStatement(duckdb_libpgquery::PGNode *stmt) {
  switch (stmt->type) {
    case duckdb_libpgquery::T_PGRawStmt:
      return BindStatement(reinterpret_cast<duckdb_libpgquery::PGRawStmt *>(stmt)->stmt);

    case duckdb_libpgquery::T_PGCreateDatabaseStmt:
      return BindCreateDatabaseStatement(reinterpret_cast<duckdb_libpgquery::PGCreateDatabaseStmt *>(stmt));
    case duckdb_libpgquery::T_PGCreateStmt:
      return BindCreateTableStatement(reinterpret_cast<duckdb_libpgquery::PGCreateStmt *>(stmt));
    case duckdb_libpgquery::T_PGIndexStmt:
      return BindCreateIndexStatement(reinterpret_cast<duckdb_libpgquery::PGIndexStmt *>(stmt));
    case duckdb_libpgquery::T_PGDropStmt:
      return BindDropStatement(reinterpret_cast<duckdb_libpgquery::PGDropStmt *>(stmt));

    case duckdb_libpgquery::T_PGInsertStmt:
      return BindInsertStatement(reinterpret_cast<duckdb_libpgquery::PGInsertStmt *>(stmt));
    case duckdb_libpgquery::T_PGDeleteStmt:
      return BindDeleteStatement(reinterpret_cast<duckdb_libpgquery::PGDeleteStmt *>(stmt));
    case duckdb_libpgquery::T_PGUpdateStmt:
      return BindUpdateStatement(reinterpret_cast<duckdb_libpgquery::PGUpdateStmt *>(stmt));
    case duckdb_libpgquery::T_PGSelectStmt:
      return BindSelectStatement(reinterpret_cast<duckdb_libpgquery::PGSelectStmt *>(stmt));
    case duckdb_libpgquery::T_PGExplainStmt:
      return BindExplainStatement(reinterpret_cast<duckdb_libpgquery::PGExplainStmt *>(stmt));

    case duckdb_libpgquery::T_PGTransactionStmt:
      return BindTransactionStatement(reinterpret_cast<duckdb_libpgquery::PGTransactionStmt *>(stmt));
    case duckdb_libpgquery::T_PGCheckPointStmt:
      return BindCheckpointStatement(reinterpret_cast<duckdb_libpgquery::PGCheckPointStmt *>(stmt));

    case duckdb_libpgquery::T_PGLockStmt:
      return BindLockStatement(reinterpret_cast<duckdb_libpgquery::PGLockStmt *>(stmt));

    case duckdb_libpgquery::T_PGVariableSetStmt:
      return BindVariableSetStatement(reinterpret_cast<duckdb_libpgquery::PGVariableSetStmt *>(stmt));
    case duckdb_libpgquery::T_PGVariableShowStmt:
      return BindVariableShowStatement(reinterpret_cast<duckdb_libpgquery::PGVariableShowStmt *>(stmt));

    case duckdb_libpgquery::T_PGCopyStmt:
      return BindCopyStatement(reinterpret_cast<duckdb_libpgquery::PGCopyStmt *>(stmt));

    case duckdb_libpgquery::T_PGVacuumStmt:
      return BindVacuumStatement(reinterpret_cast<duckdb_libpgquery::PGVacuumStmt *>(stmt));

    default:
      throw DbException("Unsupported statement type: " + NodeTagToString(stmt->type));
  }
}

std::unique_ptr<Statement> Binder::BindCreateDatabaseStatement(duckdb_libpgquery::PGCreateDatabaseStmt *stmt) {
  std::string dbname = stmt->name->relname;
  return std::make_unique<CreateDatabaseStatement>(std::move(dbname));
}

std::unique_ptr<Statement> Binder::BindCreateTableStatement(duckdb_libpgquery::PGCreateStmt *stmt) {
  std::string table_name = stmt->relation->relname;
  auto columns = std::vector<ColumnDefinition>();
  std::unordered_set<std::string> column_names;
  for (auto *col = stmt->tableElts->head; col != nullptr; col = lnext(col)) {
    auto *node = reinterpret_cast<duckdb_libpgquery::PGNode *>(col->data.ptr_value);
    switch (node->type) {
      case duckdb_libpgquery::T_PGColumnDef: {
        auto *col_def = reinterpret_cast<duckdb_libpgquery::PGColumnDef *>(col->data.ptr_value);
        auto col_entry = BindColumnDefinition(col_def);
        if (column_names.find(col_entry.GetName()) == column_names.end()) {
          column_names.insert(col_entry.GetName());
        } else {
          throw DbException("Duplicate column name: " + col_entry.GetName());
        }
        columns.push_back(std::move(col_entry));
        break;
      }
      default:
        throw DbException("Unsupported node type: " + NodeTagToString(node->type));
    }
  }
  return std::make_unique<CreateTableStatement>(std::move(table_name), std::move(columns));
}

std::unique_ptr<Statement> Binder::BindCreateIndexStatement(duckdb_libpgquery::PGIndexStmt *stmt) {
  std::string index_name = "index";
  if (stmt->idxname != nullptr) {
    index_name = stmt->idxname;
  }
  std::vector<std::string> columns;
  for (auto *node = stmt->indexParams->head; node != nullptr; node = lnext(node)) {
    auto *pg_node = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
    if (pg_node->type == duckdb_libpgquery::T_PGIndexElem) {
      auto *index_elem = reinterpret_cast<duckdb_libpgquery::PGIndexElem *>(node->data.ptr_value);
      if (index_elem->name != nullptr) {
        columns.emplace_back(index_elem->name);
      }
    } else {
      throw DbException("Unknown node type in index statement: " + NodeTagToString(pg_node->type));
    }
  }
  return std::make_unique<CreateIndexStatement>(std::move(index_name), std::move(stmt->relation->relname),
                                                std::move(columns));
}

ColumnDefinition Binder::BindColumnDefinition(duckdb_libpgquery::PGColumnDef *col_def) {
  std::string col_name = col_def->colname;
  std::string type_name =
      reinterpret_cast<duckdb_libpgquery::PGValue *>(col_def->typeName->names->tail->data.ptr_value)->val.str;
  Type col_type;
  if (type_name == "int4") {
    return {col_name, Type::INT};
  } else if (type_name == "double") {
    return {col_name, Type::DOUBLE};
  } else if (type_name == "bpchar") {
    col_type = Type::CHAR;
  } else if (type_name == "varchar") {
    col_type = Type::VARCHAR;
  } else {
    throw DbException("Unknown column type " + type_name);
  }
  if (col_def->typeName->typmods == nullptr) {
    throw DbException("Column length is not specified");
  }
  auto exprs = BindExpressionList(col_def->typeName->typmods);
  const auto &const_expr = dynamic_cast<ConstExpression &>(*exprs[0]);
  db_size_t max_size = std::stoi(const_expr.ToString());
  if (max_size <= 0) {
    throw DbException("length must be at least 1");
  }
  return {col_name, col_type, max_size};
}

std::unique_ptr<Statement> Binder::BindDropStatement(duckdb_libpgquery::PGDropStmt *stmt) {
  std::string name;
  auto *name_list = reinterpret_cast<duckdb_libpgquery::PGList *>(stmt->objects->head->data.ptr_value);
  if (name_list->length == 1) {
    name = reinterpret_cast<duckdb_libpgquery::PGValue *>(name_list->head->data.ptr_value)->val.str;
  } else {
    throw DbException("\"schema.name\" is not supported");
  }

  switch (stmt->removeType) {
    case duckdb_libpgquery::PG_OBJECT_DATABASE:
      return std::make_unique<DropDatabaseStatement>(name, stmt->missing_ok);
    case duckdb_libpgquery::PG_OBJECT_INDEX:
      return std::make_unique<DropIndexStatement>(name, stmt->missing_ok);
    case duckdb_libpgquery::PG_OBJECT_TABLE:
      return std::make_unique<DropTableStatement>(name, stmt->missing_ok);
    default:
      throw DbException("Unknown catalog type");
  }
}

std::unique_ptr<Statement> Binder::BindInsertStatement(duckdb_libpgquery::PGInsertStmt *stmt) {
  auto table = BindBaseTableRef(stmt->relation->relname, std::nullopt);
  std::vector<std::unique_ptr<ColumnRefExpression>> columns;
  if (stmt->cols != nullptr) {
    for (auto *node = stmt->cols->head; node != nullptr; node = lnext(node)) {
      auto *pg_node = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
      if (pg_node->type == duckdb_libpgquery::T_PGResTarget) {
        std::string column_name = reinterpret_cast<duckdb_libpgquery::PGResTarget *>(node->data.ptr_value)->name;
        auto column = ResolveColumnFromBaseTable(*table, {column_name});
        if (column == nullptr) {
          throw DbException(fmt::format("Column {} not found", column_name));
        }
        columns.emplace_back(std::move(column));
      } else {
        throw DbException("Unknown node type in insert statement: " + NodeTagToString(pg_node->type));
      }
    }
  }
  auto select_stmt = BindSelectStatement(reinterpret_cast<duckdb_libpgquery::PGSelectStmt *>(stmt->selectStmt));
  return std::make_unique<InsertStatement>(std::move(table), std::move(columns), std::move(select_stmt));
}

std::unique_ptr<Statement> Binder::BindDeleteStatement(duckdb_libpgquery::PGDeleteStmt *stmt) {
  auto table = BindBaseTableRef(stmt->relation->relname, std::nullopt);
  table_ = table.get();
  auto filter = std::make_unique<Expression>();
  if (stmt->whereClause != nullptr) {
    filter = BindExpression(stmt->whereClause);
  } else {
    filter = std::make_unique<ConstExpression>(Value(true));
  }
  return std::make_unique<DeleteStatement>(std::move(table), std::move(filter));
}

std::unique_ptr<Statement> Binder::BindUpdateStatement(duckdb_libpgquery::PGUpdateStmt *stmt) {
  auto table = BindBaseTableRef(stmt->relation->relname, std::nullopt);
  table_ = table.get();

  auto filter = std::make_unique<Expression>();
  if (stmt->whereClause != nullptr) {
    filter = BindExpression(stmt->whereClause);
  } else {
    filter = std::make_unique<ConstExpression>(Value(true));
  }

  std::vector<std::pair<std::unique_ptr<ColumnRefExpression>, std::unique_ptr<Expression>>> update_exprs;
  for (auto *node = stmt->targetList->head; node != nullptr; node = lnext(node)) {
    auto *target = reinterpret_cast<duckdb_libpgquery::PGResTarget *>(node->data.ptr_value);
    auto column = ResolveColumnFromBaseTable(*table, {target->name});
    update_exprs.emplace_back(std::make_pair(std::move(column), BindExpression(target->val)));
  }

  return std::make_unique<UpdateStatement>(std::move(table), std::move(filter), std::move(update_exprs));
}

std::unique_ptr<SelectStatement> Binder::BindSelectStatement(duckdb_libpgquery::PGSelectStmt *stmt) {
  if (stmt->valuesLists != nullptr) {
    auto values_list = BindValuesList(stmt->valuesLists);
    std::vector<std::unique_ptr<Expression>> exprs;
    exprs.reserve(values_list->values_list_[0].size());
    for (size_t i = 0; i < values_list->values_list_[0].size(); i++) {
      exprs.push_back(std::make_unique<ColumnRefExpression>(std::vector<std::string>{"#" + std::to_string(i + 1)}));
    }
    return std::make_unique<SelectStatement>(std::move(values_list), std::move(exprs));
  }
  auto table = BindFrom(stmt->fromClause);
  table_ = table.get();

  bool distinct = stmt->distinctClause != nullptr;

  std::unique_ptr<Expression> where = nullptr;
  if (stmt->whereClause != nullptr) {
    where = BindExpression(stmt->whereClause);
  }

  auto select_list = BindSelectList(stmt->targetList);
  adding_alias_ = false;

  auto group_by = std::vector<std::unique_ptr<Expression>>();
  if (stmt->groupClause != nullptr) {
    group_by = BindExpressionList(stmt->groupClause);
  }

  std::unique_ptr<Expression> having = nullptr;
  if (stmt->havingClause != nullptr) {
    having = BindExpression(stmt->havingClause);
  }

  auto order_by = std::vector<std::unique_ptr<OrderBy>>();
  if (stmt->sortClause != nullptr) {
    order_by = BindOrderBy(stmt->sortClause);
  }

  std::unique_ptr<Expression> limit_count = nullptr;
  if (stmt->limitCount != nullptr) {
    limit_count = BindExpression(stmt->limitCount);
  }

  std::unique_ptr<Expression> limit_offset = nullptr;
  if (stmt->limitOffset != nullptr) {
    limit_offset = BindExpression(stmt->limitOffset);
  }

  SelectLockType lock_type = SelectLockType::NOLOCK;
  if (stmt->lockingClause != nullptr) {
    if (stmt->lockingClause->length > 1) {
      throw DbException("Multiple locking clauses are not supported");
    }
    auto *node = reinterpret_cast<duckdb_libpgquery::PGNode *>(stmt->lockingClause->head->data.ptr_value);
    if (node->type == duckdb_libpgquery::T_PGLockingClause) {
      auto *lock_clause = reinterpret_cast<duckdb_libpgquery::PGLockingClause *>(node);
      if (lock_clause->strength == duckdb_libpgquery::PG_LCS_FORSHARE) {
        lock_type = SelectLockType::SHARE;
      } else if (lock_clause->strength == duckdb_libpgquery::LCS_FORUPDATE) {
        lock_type = SelectLockType::UPDATE;
      } else {
        throw DbException("Unknown lock clause strength");
      }
    } else {
      throw DbException("Unknown node type " + NodeTagToString(node->type));
    }
  }
  return std::make_unique<SelectStatement>(std::move(table), std::move(select_list), std::move(where),
                                           std::move(group_by), std::move(having), std::move(order_by),
                                           std::move(limit_count), std::move(limit_offset), lock_type, distinct);
}

std::unique_ptr<Statement> Binder::BindExplainStatement(duckdb_libpgquery::PGExplainStmt *stmt) {
  uint8_t explain_options = 0;
  if (stmt->options != nullptr) {
    for (auto *node = stmt->options->head; node != nullptr; node = lnext(node)) {
      auto *elem = reinterpret_cast<duckdb_libpgquery::PGDefElem *>(node->data.ptr_value);
      if (strcasecmp(elem->defname, "binder") == 0) {
        explain_options |= ExplainOptions::BINDER;
      } else if (strcasecmp(elem->defname, "planner") == 0) {
        explain_options |= ExplainOptions::PLANNER;
      } else if (strcasecmp(elem->defname, "optimizer") == 0) {
        explain_options |= ExplainOptions::OPTIMIZER;
      } else {
        throw DbException("Unknown explain option: " + std::string(elem->defname));
      }
    }
  } else {
    explain_options = ExplainOptions::BINDER | ExplainOptions::PLANNER | ExplainOptions::OPTIMIZER;
  }
  return std::make_unique<ExplainStatement>(BindStatement(stmt->query), explain_options);
}

std::unique_ptr<Statement> Binder::BindTransactionStatement(duckdb_libpgquery::PGTransactionStmt *stmt) {
  switch (stmt->kind) {
    case duckdb_libpgquery::PG_TRANS_STMT_BEGIN:
    case duckdb_libpgquery::PG_TRANS_STMT_START:
      return std::make_unique<TransactionStatement>(TransactionType::BEGIN);
    case duckdb_libpgquery::PG_TRANS_STMT_COMMIT:
      return std::make_unique<TransactionStatement>(TransactionType::COMMIT);
    case duckdb_libpgquery::PG_TRANS_STMT_ROLLBACK:
      return std::make_unique<TransactionStatement>(TransactionType::ROLLBACK);
    default:
      throw DbException("Unknown transaction type");
  }
}

std::unique_ptr<Statement> Binder::BindCheckpointStatement(duckdb_libpgquery::PGCheckPointStmt *stmt) {
  return std::make_unique<CheckpointStatement>();
}

std::unique_ptr<Statement> Binder::BindLockStatement(duckdb_libpgquery::PGLockStmt *stmt) {
  auto table = BindBaseTableRef(stmt->relation->relname, std::nullopt);
  TableLockType lock_type;
  if (stmt->mode == duckdb_libpgquery::PG_LCS_FORSHARE) {
    lock_type = TableLockType::SHARE;
  } else if (stmt->mode == duckdb_libpgquery::LCS_FORUPDATE) {
    lock_type = TableLockType::EXCLUSIVE;
  } else {
    throw DbException("Unknown lock mode");
  }
  return std::make_unique<LockStatement>(std::move(table), lock_type);
}

std::unique_ptr<Statement> Binder::BindVariableSetStatement(duckdb_libpgquery::PGVariableSetStmt *stmt) {
  auto expr = BindExpressionList(stmt->args);
  if (expr.size() != 1) {
    throw DbException("SET takes only one argument");
  }
  if (expr[0]->type_ != ExpressionType::CONST) {
    throw DbException("Only const is supoorted in SET");
  }
  const auto &const_expr = dynamic_cast<ConstExpression &>(*expr[0]);
  return std::make_unique<VariableSetStatement>(stmt->name, const_expr.value_.ToString());
}

std::unique_ptr<Statement> Binder::BindVariableShowStatement(duckdb_libpgquery::PGVariableShowStmt *stmt) {
  // Remove double quotes due to a bug in parser
  auto name = std::string(stmt->name);
  assert(name.size() > 2);
  name = name.substr(1, name.size() - 2);
  return std::make_unique<VariableShowStatement>(name);
}

std::unique_ptr<Statement> Binder::BindCopyStatement(duckdb_libpgquery::PGCopyStmt *stmt) {
  throw DbException("BindCopyStatement not implemented");
}

std::unique_ptr<Statement> Binder::BindVacuumStatement(duckdb_libpgquery::PGVacuumStmt *stmt) {
  if (stmt->options == duckdb_libpgquery::PG_VACOPT_VACUUM) {
    std::unique_ptr<BaseTableRef> table = nullptr;
    if (stmt->relation != nullptr) {
      table = BindBaseTableRef(stmt->relation->relname, std::nullopt);
    }
    return std::make_unique<VacuumStatement>(std::move(table));
  } else if (stmt->options == duckdb_libpgquery::PG_VACOPT_ANALYZE) {
    std::unique_ptr<BaseTableRef> table = nullptr;
    std::vector<std::unique_ptr<ColumnRefExpression>> columns;
    if (stmt->relation != nullptr) {
      table = BindBaseTableRef(stmt->relation->relname, std::nullopt);
      if (stmt->va_cols != nullptr) {
        for (auto *node = stmt->va_cols->head; node != nullptr; node = lnext(node)) {
          auto *pg_node = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
          if (pg_node->type == duckdb_libpgquery::T_PGString) {
            columns.emplace_back(ResolveColumnFromBaseTable(
                *table, {reinterpret_cast<duckdb_libpgquery::PGValue *>(node->data.ptr_value)->val.str}));
          } else {
            throw DbException("Unknown node type in vacuum statement: " + NodeTagToString(pg_node->type));
          }
        }
      }
    }
    return std::make_unique<AnalyzeStatement>(std::move(table), std::move(columns));
  } else {
    throw DbException("Only Analyze and Vacuum statement is supported now");
  }
}

std::unique_ptr<Expression> Binder::BindExpression(duckdb_libpgquery::PGNode *expr) {
  switch (expr->type) {
    case duckdb_libpgquery::T_PGAConst:
      return BindConstExpression(reinterpret_cast<duckdb_libpgquery::PGAConst *>(expr));
    case duckdb_libpgquery::T_PGResTarget:
      return BindResTargetExpression(reinterpret_cast<duckdb_libpgquery::PGResTarget *>(expr));
    case duckdb_libpgquery::T_PGColumnRef:
      return BindColumnRefExpression(reinterpret_cast<duckdb_libpgquery::PGColumnRef *>(expr));
    case duckdb_libpgquery::T_PGAExpr:
      return BindExprExpression(reinterpret_cast<duckdb_libpgquery::PGAExpr *>(expr));
    case duckdb_libpgquery::T_PGAStar:
      return BindStarExpression(reinterpret_cast<duckdb_libpgquery::PGAStar *>(expr));
    case duckdb_libpgquery::T_PGFuncCall:
      return BindFuncCallExpression(reinterpret_cast<duckdb_libpgquery::PGFuncCall *>(expr));
    case duckdb_libpgquery::T_PGBoolExpr:
      return BindBoolExpression(reinterpret_cast<duckdb_libpgquery::PGBoolExpr *>(expr));
    case duckdb_libpgquery::T_PGNullTest:
      return BindNullTestExpression(reinterpret_cast<duckdb_libpgquery::PGNullTest *>(expr));
    case duckdb_libpgquery::T_PGList:
      return BindListExpression(reinterpret_cast<duckdb_libpgquery::PGList *>(expr));
    case duckdb_libpgquery::T_PGTypeCast:
      return BindTypeCastExpression(reinterpret_cast<duckdb_libpgquery::PGTypeCast *>(expr));
    default:
      throw DbException("Unsupported expression type: " + NodeTagToString(expr->type));
  }
}

std::vector<std::unique_ptr<Expression>> Binder::BindExpressionList(duckdb_libpgquery::PGList *list) {
  std::vector<std::unique_ptr<Expression>> exprs;
  for (auto *node = list->head; node != nullptr; node = lnext(node)) {
    auto *expr = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
    exprs.push_back(BindExpression(expr));
  }
  return exprs;
}

std::unique_ptr<Expression> Binder::BindConstExpression(duckdb_libpgquery::PGAConst *expr) {
  const auto &val = expr->val;
  switch (val.type) {
    case duckdb_libpgquery::T_PGInteger:
      return std::make_unique<ConstExpression>(Value(static_cast<int32_t>(val.val.ival)));
    case duckdb_libpgquery::T_PGFloat:
      return std::make_unique<ConstExpression>(Value(std::stod(val.val.str)));
    case duckdb_libpgquery::T_PGString:
      return std::make_unique<ConstExpression>(Value(std::string(val.val.str)));
    case duckdb_libpgquery::T_PGNull:
      return std::make_unique<ConstExpression>(Value());
    default:
      throw DbException("Unsupported constant type: " + NodeTagToString(val.type));
  }
}

std::unique_ptr<Expression> Binder::BindResTargetExpression(duckdb_libpgquery::PGResTarget *expr) {
  auto bound_expr = BindExpression(expr->val);
  if (expr->name != nullptr) {
    bool add_to_alias = true;
    for (auto [it, range_end] = aliases_.equal_range(expr->name); it != range_end; it++) {
      if (it->second == bound_expr->ToString()) {
        add_to_alias = false;
        break;
      }
    }
    if (add_to_alias) {
      aliases_.emplace(expr->name, bound_expr->ToString());
    }
    return std::make_unique<AliasExpression>(expr->name, std::move(bound_expr));
  }
  return bound_expr;
}

std::unique_ptr<Expression> Binder::BindColumnRefExpression(duckdb_libpgquery::PGColumnRef *expr) {
  auto *head = static_cast<duckdb_libpgquery::PGNode *>(expr->fields->head->data.ptr_value);
  switch (head->type) {
    case duckdb_libpgquery::T_PGString: {
      std::vector<std::string> column_name;
      for (auto *node = expr->fields->head; node != nullptr; node = lnext(node)) {
        column_name.emplace_back(reinterpret_cast<duckdb_libpgquery::PGValue *>(node->data.ptr_value)->val.str);
      }
      return ResolveColumn(column_name);
    }
    case duckdb_libpgquery::T_PGAStar:
      return std::make_unique<StarExpression>();
    default:
      throw DbException("Unsupported column ref type " + NodeTagToString(head->type));
  }
}

std::unique_ptr<Expression> Binder::BindExprExpression(duckdb_libpgquery::PGAExpr *expr) {
  std::unique_ptr<Expression> left_expr = nullptr;
  std::unique_ptr<Expression> right_expr = nullptr;
  std::string name = reinterpret_cast<duckdb_libpgquery::PGValue *>(expr->name->head->data.ptr_value)->val.str;
  switch (expr->kind) {
    case duckdb_libpgquery::PG_AEXPR_OP:
    case duckdb_libpgquery::PG_AEXPR_BETWEEN:
    case duckdb_libpgquery::PG_AEXPR_NOT_BETWEEN:
    case duckdb_libpgquery::PG_AEXPR_IN:
    case duckdb_libpgquery::PG_AEXPR_LIKE: {
      if (expr->lexpr != nullptr) {
        left_expr = BindExpression(expr->lexpr);
      }
      if (expr->rexpr != nullptr) {
        right_expr = BindExpression(expr->rexpr);
      }
      if (left_expr && right_expr) {
        return std::make_unique<BinaryOpExpression>(name, std::move(left_expr), std::move(right_expr));
      }
      throw DbException("Unreachable code in BindExprExpression");
    }
    default:
      throw DbException("Unsupported PGAExpr expression type in BindExprExpression: " + name);
  }
}

std::unique_ptr<Expression> Binder::BindStarExpression(duckdb_libpgquery::PGAStar *expr) {
  return std::make_unique<StarExpression>();
}

std::unique_ptr<Expression> Binder::BindFuncCallExpression(duckdb_libpgquery::PGFuncCall *expr) {
  std::string function_name =
      reinterpret_cast<duckdb_libpgquery::PGValue *>(expr->funcname->head->data.ptr_value)->val.str;
  std::transform(function_name.begin(), function_name.end(), function_name.begin(),
                 [](unsigned char character) { return std::tolower(character); });

  std::vector<std::unique_ptr<Expression>> args;
  if (expr->args != nullptr) {
    for (auto *node = expr->args->head; node != nullptr; node = lnext(node)) {
      auto arg = BindExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value));
      args.push_back(std::move(arg));
    }
  }

  if (function_name == "avg" || function_name == "count" || function_name == "sum" || function_name == "min" ||
      function_name == "max") {
    return std::make_unique<AggregateExpression>(std::move(function_name), expr->agg_distinct, std::move(args));
  }
  if (function_name == "lower" || function_name == "upper" || function_name == "length") {
    if (expr->agg_distinct) {
      throw DbException(fmt::format("DISTINCT specified, but {} is not an aggregate function", function_name));
    }
    return std::make_unique<FuncCallExpression>(std::move(function_name), std::move(args));
  }
  throw DbException("Unsupported function call: " + function_name);
}

std::unique_ptr<Expression> Binder::BindBoolExpression(duckdb_libpgquery::PGBoolExpr *expr) {
  switch (expr->boolop) {
    case duckdb_libpgquery::PG_AND_EXPR:
    case duckdb_libpgquery::PG_OR_EXPR: {
      std::string op_name;
      if (expr->boolop == duckdb_libpgquery::PG_AND_EXPR) {
        op_name = "and";
      } else if (expr->boolop == duckdb_libpgquery::PG_OR_EXPR) {
        op_name = "or";
      } else {
        throw DbException("Unknown boolop type in BindBoolExpression");
      }

      auto exprs = BindExpressionList(expr->args);
      if (exprs.size() < 2) {
        throw DbException("Too few arguments in BindBoolExpression");
      }

      auto binary_expr = std::make_unique<BinaryOpExpression>(op_name, std::move(exprs[0]), std::move(exprs[1]));
      for (size_t i = 2; i < exprs.size(); i++) {
        binary_expr = std::make_unique<BinaryOpExpression>(op_name, std::move(binary_expr), std::move(exprs[i]));
      }
      return binary_expr;
    }
    case duckdb_libpgquery::PG_NOT_EXPR: {
      auto exprs = BindExpressionList(expr->args);
      if (exprs.size() != 1) {
        throw DbException("Not Expr should have only one argument");
      }
      return std::make_unique<UnaryOpExpression>("not", std::move(exprs[0]));
    }
    default:
      throw DbException("Unsupported PGBoolExpr expression type in BindBoolExpression");
  }
}

std::unique_ptr<Expression> Binder::BindNullTestExpression(duckdb_libpgquery::PGNullTest *expr) {
  auto *null_expr = reinterpret_cast<duckdb_libpgquery::PGNullTest *>(expr);
  auto arg = BindExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(null_expr->arg));
  bool is_null;
  switch (null_expr->nulltesttype) {
    case duckdb_libpgquery::PG_IS_NULL:
      is_null = true;
      break;
    case duckdb_libpgquery::IS_NOT_NULL:
      is_null = false;
      break;
    default:
      throw DbException("Unsupported null test type");
  }
  return std::make_unique<NullTestExpression>(is_null, std::move(arg));
}

std::unique_ptr<Expression> Binder::BindListExpression(duckdb_libpgquery::PGList *expr) {
  std::vector<std::unique_ptr<Expression>> exprs;
  auto *list = reinterpret_cast<duckdb_libpgquery::PGList *>(expr);
  for (auto *node = list->head; node != nullptr; node = lnext(node)) {
    auto *item = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
    exprs.push_back(BindExpression(item));
  }
  return std::make_unique<ListExpression>(std::move(exprs));
}

std::unique_ptr<Expression> Binder::BindTypeCastExpression(duckdb_libpgquery::PGTypeCast *expr) {
  auto *type_cast = reinterpret_cast<duckdb_libpgquery::PGTypeCast *>(expr);
  std::string type_name =
      reinterpret_cast<duckdb_libpgquery::PGValue *>(type_cast->typeName->names->tail->data.ptr_value)->val.str;
  auto arg = BindExpression(type_cast->arg);
  return std::make_unique<TypeCastExpression>(TypeUtil::String2Type(type_name), std::move(arg));
}

std::vector<std::unique_ptr<OrderBy>> Binder::BindOrderBy(duckdb_libpgquery::PGList *list) {
  auto order_by = std::vector<std::unique_ptr<OrderBy>>();
  for (auto *node = list->head; node != nullptr; node = lnext(node)) {
    auto *item = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
    if (item->type == duckdb_libpgquery::T_PGSortBy) {
      OrderByType type;
      auto *sort = reinterpret_cast<duckdb_libpgquery::PGSortBy *>(item);
      if (sort->sortby_dir == duckdb_libpgquery::PG_SORTBY_DEFAULT) {
        type = OrderByType::DEFAULT;
      } else if (sort->sortby_dir == duckdb_libpgquery::PG_SORTBY_ASC) {
        type = OrderByType::ASC;
      } else if (sort->sortby_dir == duckdb_libpgquery::PG_SORTBY_DESC) {
        type = OrderByType::DESC;
      } else {
        throw DbException("Unknown sort by type");
      }
      auto *target = sort->node;
      auto expr = BindExpression(target);
      order_by.emplace_back(std::make_unique<OrderBy>(type, std::move(expr)));
    } else {
      throw DbException("Unsupported order by type: " + NodeTagToString(item->type));
    }
  }
  return order_by;
}

std::unique_ptr<TableRef> Binder::BindTableRef(duckdb_libpgquery::PGNode *ref) {
  switch (ref->type) {
    case duckdb_libpgquery::T_PGRangeVar:
      return BindRangeVar(reinterpret_cast<duckdb_libpgquery::PGRangeVar *>(ref));
    case duckdb_libpgquery::T_PGJoinExpr:
      return BindJoin(reinterpret_cast<duckdb_libpgquery::PGJoinExpr *>(ref));
    default:
      throw DbException("Unsupported table reference type: " + NodeTagToString(ref->type));
  }
}

std::unique_ptr<TableRef> Binder::BindFrom(duckdb_libpgquery::PGList *list) {
  if (list == nullptr) {
    return std::make_unique<TableRef>(TableRefType::EMPTY);
  }
  if (list->length > 1) {
    auto *node = list->head;
    auto *left_node = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
    auto left_table = BindTableRef(left_node);
    node = lnext(node);

    auto *right_node = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
    auto right_table = BindTableRef(right_node);
    node = lnext(node);

    auto result = std::make_unique<CrossJoinRef>(std::move(left_table), std::move(right_table));
    for (; node != nullptr; node = lnext(node)) {
      auto *table_node = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
      auto table = BindTableRef(table_node);
      result = std::make_unique<CrossJoinRef>(std::move(result), std::move(table));
    }
    return result;
  }
  auto *ref = reinterpret_cast<duckdb_libpgquery::PGNode *>(list->head->data.ptr_value);
  return BindTableRef(ref);
}

std::unique_ptr<TableRef> Binder::BindRangeVar(duckdb_libpgquery::PGRangeVar *ref) {
  if (ref->alias != nullptr) {
    return BindBaseTableRef(ref->relname, std::make_optional(ref->alias->aliasname));
  }
  return BindBaseTableRef(ref->relname, std::nullopt);
}

std::unique_ptr<TableRef> Binder::BindJoin(duckdb_libpgquery::PGJoinExpr *ref) {
  JoinType join_type;
  switch (ref->jointype) {
    case duckdb_libpgquery::PG_JOIN_INNER:
      join_type = JoinType::INNER;
      break;
    case duckdb_libpgquery::PG_JOIN_LEFT:
      join_type = JoinType::LEFT;
      break;
    case duckdb_libpgquery::PG_JOIN_RIGHT:
      join_type = JoinType::RIGHT;
      break;
    case duckdb_libpgquery::PG_JOIN_FULL:
      join_type = JoinType::FULL;
      break;
    default:
      throw DbException("Unsupported join type");
      break;
  }
  auto left = BindTableRef(ref->larg);
  auto right = BindTableRef(ref->rarg);
  auto join_ref = std::make_unique<JoinRef>(join_type, std::move(left), std::move(right), nullptr);
  table_ = join_ref.get();
  std::unique_ptr<Expression> condition;
  if (ref->quals != nullptr) {
    condition = BindExpression(ref->quals);
  } else if (ref->usingClause != nullptr) {
    if (ref->usingClause->length != 1) {
      throw DbException("Using clause only support one column now");
    }
    std::string column_name =
        reinterpret_cast<duckdb_libpgquery::PGValue *>(ref->usingClause->head->data.ptr_value)->val.str;
    condition = std::make_unique<BinaryOpExpression>(
        "=", ResolveColumnInternal(*join_ref->left_, std::vector<std::string>{column_name}),
        ResolveColumnInternal(*join_ref->right_, std::vector<std::string>{column_name}));
  } else {
    throw DbException("Join condition is not specified");
  }
  join_ref->condition_ = std::move(condition);
  return join_ref;
}

std::unique_ptr<BaseTableRef> Binder::BindBaseTableRef(std::string table_name, std::optional<std::string> alias) {
  if (alias) {
    if (table_names_.find(*alias) != table_names_.end()) {
      throw DbException(fmt::format("Table name \"{}\" specified more than once", *alias));
    }
    table_names_.insert(*alias);
  } else {
    if (table_names_.find(table_name) != table_names_.end()) {
      throw DbException(fmt::format("Table name \"{}\" specified more than once", table_name));
    }
    table_names_.insert(table_name);
  }
  auto oid = catalog_.GetTableOid(table_name);
  auto column_list = catalog_.GetTableColumnList(table_name);
  return std::make_unique<BaseTableRef>(std::move(table_name), std::move(alias), oid, column_list);
}

std::vector<std::unique_ptr<Expression>> Binder::BindSelectList(duckdb_libpgquery::PGList *list) {
  if (list == nullptr) {
    throw DbException("Select list is empty");
  }
  std::vector<std::unique_ptr<Expression>> exprs;
  for (auto *node = list->head; node != nullptr; node = lnext(node)) {
    auto *expr = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
    auto bound_expr = BindExpression(expr);
    if (bound_expr->type_ == ExpressionType::STAR) {
      exprs = GetAllColumns(*table_);
    } else {
      exprs.push_back(std::move(bound_expr));
    }
  }
  return exprs;
}

std::unique_ptr<ExpressionListRef> Binder::BindValuesList(duckdb_libpgquery::PGList *list) {
  std::vector<std::vector<std::unique_ptr<Expression>>> values_list;
  for (auto *node = list->head; node != nullptr; node = lnext(node)) {
    auto values = BindExpressionList(static_cast<duckdb_libpgquery::PGList *>(node->data.ptr_value));
    values_list.push_back(std::move(values));
  }
  return std::make_unique<ExpressionListRef>(std::move(values_list));
}

std::unique_ptr<Expression> Binder::ResolveColumn(const std::vector<std::string> &column_name) {
  auto column = ResolveColumnInternal(*table_, column_name);
  // 别名与列名重合
  if (column != nullptr && !adding_alias_ && column_name.size() == 1 &&
      aliases_.find(column_name[0]) != aliases_.end()) {
    throw DbException(fmt::format("Column reference {} is ambiguous 1", column_name[0]));
  }
  if (column == nullptr) {
    if (column_name.size() != 1 || aliases_.find(column_name[0]) == aliases_.end()) {
      throw DbException(fmt::format("Column {} not found", fmt::join(column_name, ".")));
    }
    // 别名重合
    if (aliases_.count(column_name[0]) > 1) {
      throw DbException(fmt::format("Column reference {} is ambiguous 2", column_name[0]));
    }
    return std::make_unique<ColumnRefExpression>(std::vector<std::string>{column_name[0]});
  }
  return column;
}

std::unique_ptr<ColumnRefExpression> Binder::ResolveColumnFromBaseTable(const BaseTableRef &base_table,
                                                                        const std::vector<std::string> &column_name) {
  if (column_name.size() > 1) {
    if (column_name[0] == base_table.GetTableName()) {
      return std::make_unique<ColumnRefExpression>(column_name);
    }
  } else if (column_name.size() == 1) {
    if (base_table.column_list_.TryGetColumnIndex(column_name[0])) {
      std::vector<std::string> table_column_name = {base_table.GetTableName(), column_name[0]};
      return std::make_unique<ColumnRefExpression>(table_column_name);
    }
  } else {
    throw DbException("Empty column name");
  }
  return nullptr;
}

std::unique_ptr<ColumnRefExpression> Binder::ResolveColumnInternal(const TableRef &table,
                                                                   const std::vector<std::string> &column_name) {
  switch (table.type_) {
    case TableRefType::BASE_TABLE: {
      const auto &base_table = dynamic_cast<const BaseTableRef &>(table);
      return ResolveColumnFromBaseTable(base_table, column_name);
    }
    case TableRefType::CROSS_JOIN: {
      const auto &cross_join = dynamic_cast<const CrossJoinRef &>(table);
      auto left = ResolveColumnInternal(*cross_join.left_, column_name);
      auto right = ResolveColumnInternal(*cross_join.right_, column_name);
      if (left && right) {
        throw DbException(fmt::format("Column reference {} is ambiguous 3", fmt::join(column_name, ".")));
      }
      if (left) {
        return left;
      }
      return right;
    }
    case TableRefType::JOIN: {
      const auto &join = dynamic_cast<const JoinRef &>(table);
      auto left = ResolveColumnInternal(*join.left_, column_name);
      auto right = ResolveColumnInternal(*join.right_, column_name);
      if (left && right) {
        throw DbException(fmt::format("Column reference {} is ambiguous 4", fmt::join(column_name, ".")));
      }
      if (left) {
        return left;
      }
      return right;
    }
    default:
      throw DbException("Unsupported table ref type in ResolveColumn");
  }
  throw DbException("ResolveColumnInternal not implemented yet");
}

std::vector<std::unique_ptr<Expression>> Binder::GetAllColumns(const TableRef &table) {
  switch (table.type_) {
    case TableRefType::BASE_TABLE: {
      const auto &base_table = dynamic_cast<const BaseTableRef &>(table);
      std::vector<std::unique_ptr<Expression>> columns;
      for (const auto &column : base_table.column_list_.GetColumns()) {
        columns.push_back(std::make_unique<ColumnRefExpression>(std::vector{base_table.GetTableName(), column.name_}));
      }
      return columns;
    }
    case TableRefType::CROSS_JOIN: {
      const auto &cross_join = dynamic_cast<const CrossJoinRef &>(table);
      auto columns = GetAllColumns(*cross_join.left_);
      auto right_columns = GetAllColumns(*cross_join.right_);
      for (auto &column : right_columns) {
        columns.push_back(std::move(column));
      }
      return columns;
    }
    case TableRefType::JOIN: {
      const auto &join = dynamic_cast<const JoinRef &>(table);
      auto columns = GetAllColumns(*join.left_);
      auto right_columns = GetAllColumns(*join.right_);
      for (auto &column : right_columns) {
        columns.push_back(std::move(column));
      }
      return columns;
    }
    default:
      throw DbException("Unsupported table type in GetAllColumns");
  }
}

}  // namespace huadb
