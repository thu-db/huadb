#pragma once

#include "binder/expression.h"
#include "fmt/format.h"

namespace huadb {

class AggregateExpression : public Expression {
 public:
  AggregateExpression(std::string function_name, std::vector<std::unique_ptr<Expression>> args)
      : Expression(ExpressionType::AGGREGATE), function_name_(std::move(function_name)), args_(std::move(args)) {}
  std::string ToString() const override { return fmt::format("{}", function_name_); }
  bool HasAggregation() const override { return true; }

  std::string function_name_;
  std::vector<std::unique_ptr<Expression>> args_;
};

}  // namespace huadb
