#pragma once

#include "binder/statement.h"

namespace huadb {

class VariableShowStatement : public Statement {
 public:
  explicit VariableShowStatement(std::string variable)
      : Statement(StatementType::VARIABLE_SHOW_STATEMENT), variable_(std::move(variable)) {}
  std::string ToString() const override { return fmt::format("VariableShowStatement: variable: {}\n", variable_); }

  std::string variable_;
};

}  // namespace huadb
