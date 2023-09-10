#pragma once

#include <vector>

#include "binder/expressions/column_ref_expression.h"
#include "binder/statement.h"
#include "binder/table_refs/base_table_ref.h"
#include "fmt/format.h"

namespace huadb {

class AnalyzeStatement : public Statement {
 public:
  AnalyzeStatement(std::unique_ptr<BaseTableRef> table, std::vector<std::unique_ptr<ColumnRefExpression>> columns)
      : Statement(StatementType::ANALYZE_STATEMENT), table_(std::move(table)), columns_(std::move(columns)) {}
  std::string ToString() const override { return fmt::format("AnalyzeStatement: {}\n", table_); }

  std::unique_ptr<BaseTableRef> table_;
  std::vector<std::unique_ptr<ColumnRefExpression>> columns_;
};

}  // namespace huadb
