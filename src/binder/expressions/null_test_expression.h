#pragma once

#include "binder/expression.h"

namespace huadb {

class NullTestExpression : public Expression {
 public:
  NullTestExpression(bool is_null, std::unique_ptr<Expression> arg)
      : Expression(ExpressionType::NULL_TEST), is_null_(is_null), arg_(std::move(arg)) {}
  std::string ToString() const override { return arg_->ToString(); }
  bool HasAggregation() const override { return false; }

  bool is_null_;
  std::unique_ptr<Expression> arg_;
};

}  // namespace huadb
