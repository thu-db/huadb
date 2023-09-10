#pragma once

#include <utility>
#include <vector>

#include "binder/expressions/column_ref_expression.h"
#include "binder/statement.h"
#include "binder/table_refs/base_table_ref.h"
#include "fmt/format.h"

namespace huadb {

class UpdateStatement : public Statement {
 public:
  UpdateStatement(
      std::unique_ptr<BaseTableRef> table, std::unique_ptr<Expression> filter,
      std::vector<std::pair<std::unique_ptr<ColumnRefExpression>, std::unique_ptr<Expression>>> update_exprs)
      : Statement(StatementType::UPDATE_STATEMENT),
        table_(std::move(table)),
        filter_(std::move(filter)),
        update_exprs_(std::move(update_exprs)) {}
  std::string ToString() const override {
    return fmt::format("UpdateStatement: table={}, filter={}\n", table_, filter_);
  }
  std::unique_ptr<BaseTableRef> table_;
  std::unique_ptr<Expression> filter_;
  std::vector<std::pair<std::unique_ptr<ColumnRefExpression>, std::unique_ptr<Expression>>> update_exprs_;
};

}  // namespace huadb
