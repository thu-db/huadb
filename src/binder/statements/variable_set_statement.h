#pragma once

#include "binder/statement.h"
#include "fmt/format.h"

namespace huadb {

class VariableSetStatement : public Statement {
 public:
  VariableSetStatement(std::string variable, std::string value)
      : Statement(StatementType::VARIABLE_SET_STATEMENT), variable_(std::move(variable)), value_(std::move(value)) {}
  std::string ToString() const override {
    return fmt::format("VariableSetStatement: variable={}, value={}\n", variable_, value_);
  }

  std::string variable_;
  std::string value_;
};

}  // namespace huadb
