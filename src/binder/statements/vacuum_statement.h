#pragma once

#include <vector>

#include "binder/statement.h"
#include "binder/table_refs/base_table_ref.h"
#include "fmt/format.h"

namespace huadb {

class VacuumStatement : public Statement {
 public:
  explicit VacuumStatement(std::unique_ptr<BaseTableRef> table)
      : Statement(StatementType::VACUUM_STATEMENT), table_(std::move(table)) {}
  std::string ToString() const override { return fmt::format("VacuumStatement: {}\n", table_); }

  std::unique_ptr<BaseTableRef> table_;
};

}  // namespace huadb
