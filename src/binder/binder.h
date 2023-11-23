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

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "postgres_parser.hpp"

namespace duckdb_libpgquery {
struct PGCreateDatabaseStmt;
struct PGUseStmt;
struct PGDropStmt;
struct PGCreateStmt;
struct PGIndexStmt;

struct PGInsertStmt;
struct PGDeleteStmt;
struct PGUpdateStmt;
struct PGSelectStmt;

struct PGTransactionStmt;
struct PGCheckPointStmt;

struct PGVacuumStmt;
struct PGExplainStmt;

struct PGColumnDef;

struct PGAConst;
struct PGResTarget;
struct PGAExpr;
struct PGAStar;
struct PGBoolExpr;
struct PGNullTest;
struct PGList;
struct PGTypeCast;
struct PGColumnRef;
struct PGFuncCall;

struct PGRangeVar;
struct PGJoinExpr;

struct PGLockStmt;

struct PGVariableSetStmt;
struct PGVariableShowStmt;

struct PGCopyStmt;
}  // namespace duckdb_libpgquery

namespace huadb {

class Statement;
class SelectStatement;
class ColumnDefinition;
class Expression;
class BaseTableRef;
class TableRef;
class ExpressionListRef;
class ColumnRefExpression;

class OrderBy;

class Binder {
 public:
  explicit Binder(Catalog &catalog) : catalog_(catalog) {}

  std::unique_ptr<Statement> BindStatement(duckdb_libpgquery::PGNode *stmt);

  std::unique_ptr<Statement> BindCreateDatabaseStatement(duckdb_libpgquery::PGCreateDatabaseStmt *stmt);
  std::unique_ptr<Statement> BindCreateTableStatement(duckdb_libpgquery::PGCreateStmt *stmt);
  std::unique_ptr<Statement> BindCreateIndexStatement(duckdb_libpgquery::PGIndexStmt *stmt);
  std::unique_ptr<Statement> BindDropStatement(duckdb_libpgquery::PGDropStmt *stmt);

  std::unique_ptr<Statement> BindInsertStatement(duckdb_libpgquery::PGInsertStmt *stmt);
  std::unique_ptr<Statement> BindDeleteStatement(duckdb_libpgquery::PGDeleteStmt *stmt);
  std::unique_ptr<Statement> BindUpdateStatement(duckdb_libpgquery::PGUpdateStmt *stmt);
  std::unique_ptr<SelectStatement> BindSelectStatement(duckdb_libpgquery::PGSelectStmt *stmt);
  std::unique_ptr<Statement> BindExplainStatement(duckdb_libpgquery::PGExplainStmt *stmt);

  std::unique_ptr<Statement> BindTransactionStatement(duckdb_libpgquery::PGTransactionStmt *stmt);
  std::unique_ptr<Statement> BindCheckpointStatement(duckdb_libpgquery::PGCheckPointStmt *stmt);

  std::unique_ptr<Statement> BindLockStatement(duckdb_libpgquery::PGLockStmt *stmt);

  std::unique_ptr<Statement> BindVariableSetStatement(duckdb_libpgquery::PGVariableSetStmt *stmt);
  std::unique_ptr<Statement> BindVariableShowStatement(duckdb_libpgquery::PGVariableShowStmt *stmt);

  std::unique_ptr<Statement> BindCopyStatement(duckdb_libpgquery::PGCopyStmt *stmt);

  std::unique_ptr<Statement> BindVacuumStatement(duckdb_libpgquery::PGVacuumStmt *stmt);

 private:
  static std::string NodeTagToString(duckdb_libpgquery::PGNodeTag tag);

  std::unique_ptr<Expression> BindExpression(duckdb_libpgquery::PGNode *expr);
  std::vector<std::unique_ptr<Expression>> BindExpressionList(duckdb_libpgquery::PGList *list);

  std::unique_ptr<Expression> BindConstExpression(duckdb_libpgquery::PGAConst *expr);
  std::unique_ptr<Expression> BindResTargetExpression(duckdb_libpgquery::PGResTarget *expr);
  std::unique_ptr<Expression> BindColumnRefExpression(duckdb_libpgquery::PGColumnRef *expr);
  std::unique_ptr<Expression> BindExprExpression(duckdb_libpgquery::PGAExpr *expr);
  std::unique_ptr<Expression> BindStarExpression(duckdb_libpgquery::PGAStar *expr);
  std::unique_ptr<Expression> BindFuncCallExpression(duckdb_libpgquery::PGFuncCall *expr);
  std::unique_ptr<Expression> BindBoolExpression(duckdb_libpgquery::PGBoolExpr *expr);
  std::unique_ptr<Expression> BindNullTestExpression(duckdb_libpgquery::PGNullTest *expr);
  std::unique_ptr<Expression> BindListExpression(duckdb_libpgquery::PGList *expr);
  std::unique_ptr<Expression> BindTypeCastExpression(duckdb_libpgquery::PGTypeCast *expr);

  std::vector<std::unique_ptr<OrderBy>> BindOrderBy(duckdb_libpgquery::PGList *list);

  std::unique_ptr<TableRef> BindTableRef(duckdb_libpgquery::PGNode *ref);
  std::unique_ptr<TableRef> BindFrom(duckdb_libpgquery::PGList *list);

  std::unique_ptr<TableRef> BindRangeVar(duckdb_libpgquery::PGRangeVar *ref);
  std::unique_ptr<TableRef> BindJoin(duckdb_libpgquery::PGJoinExpr *ref);

  ColumnDefinition BindColumnDefinition(duckdb_libpgquery::PGColumnDef *col_def);
  std::unique_ptr<BaseTableRef> BindBaseTableRef(std::string table_name, std::optional<std::string> alias);
  std::vector<std::unique_ptr<Expression>> BindSelectList(duckdb_libpgquery::PGList *list);
  std::unique_ptr<ExpressionListRef> BindValuesList(duckdb_libpgquery::PGList *list);

  std::unique_ptr<Expression> ResolveColumn(const std::vector<std::string> &column_name);
  std::unique_ptr<ColumnRefExpression> ResolveColumnFromBaseTable(const BaseTableRef &base_table,
                                                                  const std::vector<std::string> &column_name);
  std::unique_ptr<ColumnRefExpression> ResolveColumnInternal(const TableRef &table,
                                                             const std::vector<std::string> &column_name);
  std::vector<std::unique_ptr<Expression>> GetAllColumns(const TableRef &table);

  Catalog &catalog_;
  const TableRef *table_ = nullptr;
  std::unordered_multimap<std::string, std::string> aliases_;
  std::unordered_set<std::string> table_names_;
  bool adding_alias_ = true;
};

}  // namespace huadb
