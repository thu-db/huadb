#pragma once

#include <string>

#include "binder/statement.h"
#include "fmt/ranges.h"

namespace huadb {

class CreateTableStatement : public Statement {
 public:
  CreateTableStatement(std::string table, std::vector<ColumnDefinition> columns)
      : Statement(StatementType::CREATE_TABLE_STATEMENT), table_(std::move(table)), columns_(std::move(columns)) {}
  std::string ToString() const override {
    return fmt::format("CreateTableStatement: table={} columns={}\n", table_, columns_);
  }
  std::string table_;
  std::vector<ColumnDefinition> columns_;
};

}  // namespace huadb
