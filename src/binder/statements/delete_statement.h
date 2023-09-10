#pragma once

#include "binder/statement.h"
#include "binder/table_refs/base_table_ref.h"
#include "fmt/format.h"

namespace huadb {

class DeleteStatement : public Statement {
 public:
  DeleteStatement(std::unique_ptr<BaseTableRef> table, std::unique_ptr<Expression> filter)
      : Statement(StatementType::DELETE_STATEMENT), table_(std::move(table)), filter_(std::move(filter)) {}
  std::string ToString() const override { return fmt::format("DeleteStatement: table={} filter={}", table_, filter_); }
  std::unique_ptr<BaseTableRef> table_;
  std::unique_ptr<Expression> filter_;
};

}  // namespace huadb
