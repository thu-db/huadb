#pragma once

#include <string>

#include "binder/statement.h"
#include "fmt/format.h"

namespace huadb {

class CreateDatabaseStatement : public Statement {
 public:
  explicit CreateDatabaseStatement(std::string database)
      : Statement(StatementType::CREATE_DATABASE_STATEMENT), database_(std::move(database)) {}
  std::string ToString() const override { return fmt::format("CreateDatabaseStatement: database={}\n", database_); }
  std::string database_;
};

}  // namespace huadb
