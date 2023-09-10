#pragma once

#include "operators/expressions/expression.h"

namespace huadb {

class Const : public OperatorExpression {
 public:
  explicit Const(const Value &value)
      : OperatorExpression(OperatorExpressionType::CONST, {}, value.GetType(), "<no_name>", value.GetSize()),
        value_(value) {}
  Value Evaluate(std::shared_ptr<const Record> record) override { return value_; }
  Value EvaluateJoin(std::shared_ptr<const Record> left, std::shared_ptr<const Record> right) override {
    return value_;
  }
  std::string ToString() const override { return value_.ToString(); }
  Value value_;
};

}  // namespace huadb
