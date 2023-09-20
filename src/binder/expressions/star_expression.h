#pragma once

#include "binder/expression.h"

namespace huadb {

class StarExpression : public Expression {
 public:
  StarExpression() : Expression(ExpressionType::STAR) {}
  std::string ToString() const override { return "*"; }
  bool HasAggregation() const override { return false; }
};

}  // namespace huadb
