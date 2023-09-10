#pragma once

#include <string>

#include "binder/statement.h"
#include "fmt/format.h"

namespace huadb {

class DropTableStatement : public Statement {
 public:
  DropTableStatement(std::string table, bool missing_ok)
      : Statement(StatementType::DROP_TABLE_STATEMENT), table_(std::move(table)), missing_ok_(missing_ok) {}
  std::string ToString() const override { return fmt::format("DropTableStatement: table={}\n", table_); }
  std::string table_;
  bool missing_ok_;
};

}  // namespace huadb
