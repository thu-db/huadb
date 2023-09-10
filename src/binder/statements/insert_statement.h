#pragma once

#include <vector>

#include "binder/statement.h"
#include "binder/statements/select_statement.h"
#include "binder/table_refs/base_table_ref.h"
#include "fmt/format.h"

namespace huadb {

class InsertStatement : public Statement {
 public:
  InsertStatement(std::unique_ptr<BaseTableRef> table, std::vector<std::unique_ptr<ColumnRefExpression>> columns,
                  std::unique_ptr<SelectStatement> select_stmt)
      : Statement(StatementType::INSERT_STATEMENT),
        table_(std::move(table)),
        columns_(std::move(columns)),
        select_stmt_(std::move(select_stmt)) {}
  std::string ToString() const override {
    return fmt::format("InsertStatement: table={}, select=\n{}", table_, select_stmt_->ToString());
  }
  std::unique_ptr<BaseTableRef> table_;
  std::vector<std::unique_ptr<ColumnRefExpression>> columns_;
  std::unique_ptr<SelectStatement> select_stmt_;
};

}  // namespace huadb
