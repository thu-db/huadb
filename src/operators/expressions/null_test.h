#pragma once

#include "operators/expressions/expression.h"

namespace huadb {

class NullTest : public OperatorExpression {
 public:
  NullTest(bool is_null, std::shared_ptr<OperatorExpression> arg)
      : OperatorExpression(OperatorExpressionType::NULL_TEST, {}, Type::BOOL, "<no_name>"),
        is_null_(is_null),
        arg_(std::move(arg)) {}
  Value Evaluate(std::shared_ptr<const Record> record) override {
    auto value = arg_->Evaluate(record);
    if (is_null_) {
      return Value(value.IsNull());
    } else {
      return Value(!value.IsNull());
    }
  }
  Value EvaluateJoin(std::shared_ptr<const Record> left, std::shared_ptr<const Record> right) override {
    auto value = arg_->Evaluate(left);
    if (is_null_) {
      return Value(value.IsNull());
    } else {
      return Value(!value.IsNull());
    }
  }
  std::string ToString() const override { return arg_->ToString(); }
  bool is_null_;
  std::shared_ptr<OperatorExpression> arg_;
};

}  // namespace huadb
