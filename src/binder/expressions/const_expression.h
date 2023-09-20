#pragma once

#include "binder/expression.h"
#include "common/value.h"

namespace huadb {

class ConstExpression : public Expression {
 public:
  explicit ConstExpression(Value value) : Expression(ExpressionType::CONST), value_(std::move(value)) {}
  std::string ToString() const override { return value_.ToString(); }
  bool HasAggregation() const override { return false; }
  Value value_;
};

}  // namespace huadb
