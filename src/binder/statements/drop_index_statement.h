#pragma once

#include <string>

#include "binder/statement.h"
#include "fmt/format.h"

namespace huadb {

class DropIndexStatement : public Statement {
 public:
  DropIndexStatement(std::string index_name, bool missing_ok)
      : Statement(StatementType::DROP_INDEX_STATEMENT), index_name_(std::move(index_name)), missing_ok_(missing_ok) {}
  std::string ToString() const override { return fmt::format("DropIndexStatement: index_name={}\n", index_name_); }
  std::string index_name_;
  bool missing_ok_;
};

}  // namespace huadb
