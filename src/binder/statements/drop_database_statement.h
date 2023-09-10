#pragma once

#include <string>

#include "binder/statement.h"
#include "fmt/format.h"

namespace huadb {

class DropDatabaseStatement : public Statement {
 public:
  DropDatabaseStatement(std::string database, bool missing_ok)
      : Statement(StatementType::DROP_DATABASE_STATEMENT), database_(std::move(database)), missing_ok_(missing_ok) {}
  std::string ToString() const override { return "DropDatabaseStatement" + database_ + "\n"; }
  std::string database_;
  bool missing_ok_;
};

}  // namespace huadb
