#pragma once

#include <vector>

#include "binder/expression.h"
#include "fmt/ranges.h"

namespace huadb {

class ListExpression : public Expression {
 public:
  ListExpression(std::vector<std::unique_ptr<Expression>> exprs)
      : Expression(ExpressionType::LIST), exprs_(std::move(exprs)) {}
  std::string ToString() const override { return fmt::format("{}", exprs_); }
  bool HasAggregation() const override { return false; }

  std::vector<std::unique_ptr<Expression>> exprs_;
};

}  // namespace huadb
