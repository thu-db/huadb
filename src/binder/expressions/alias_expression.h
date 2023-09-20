#pragma once

#include "binder/expression.h"
#include "fmt/format.h"

namespace huadb {

class AliasExpression : public Expression {
 public:
  AliasExpression(std::string name, std::shared_ptr<Expression> expr)
      : Expression(ExpressionType::ALIAS), name_(std::move(name)), expr_(std::move(expr)) {}
  std::string ToString() const override { return fmt::format("{} as {}", expr_, name_); }
  bool HasAggregation() const override { return expr_->HasAggregation(); }

  std::string name_;
  std::shared_ptr<Expression> expr_;
};

}  // namespace huadb
