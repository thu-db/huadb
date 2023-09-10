#pragma once

#include <memory>

#include "binder/statement.h"
#include "fmt/format.h"

namespace huadb {

enum ExplainOptions {
  BINDER = 1,
  PLANNER = 2,
  OPTIMIZER = 4,
};

class ExplainStatement : public Statement {
 public:
  ExplainStatement(std::unique_ptr<Statement> statement, uint8_t options)
      : Statement(StatementType::EXPLAIN_STATEMENT), statement_(std::move(statement)), options_(options) {}
  std::string ToString() const override {
    return fmt::format("ExplainStatement:\n statement={}\n", statement_->ToString());
  }
  std::unique_ptr<Statement> statement_;
  uint8_t options_;
};

}  // namespace huadb
