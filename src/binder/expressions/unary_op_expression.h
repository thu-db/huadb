#pragma once

#include <memory>
#include <string>

#include "binder/expression.h"
#include "fmt/format.h"

namespace huadb {

class UnaryOpExpression : public Expression {
 public:
  UnaryOpExpression(std::string op_name, std::unique_ptr<Expression> arg)
      : Expression(ExpressionType::UNARY_OP), op_name_(std::move(op_name)), arg_(std::move(arg)) {}
  std::string ToString() const override { return fmt::format("{} {}", op_name_, arg_); }
  bool HasAggregation() const override { return arg_->HasAggregation(); }

  std::string op_name_;
  std::unique_ptr<Expression> arg_;
};

}  // namespace huadb
