#pragma once

#include <memory>
#include <string>

#include "binder/expression.h"
#include "fmt/format.h"

namespace huadb {

class BinaryOpExpression : public Expression {
 public:
  BinaryOpExpression(std::string op_name, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right)
      : Expression(ExpressionType::BINARY_OP),
        op_name_(std::move(op_name)),
        left_(std::move(left)),
        right_(std::move(right)) {}
  std::string ToString() const override { return fmt::format("{} {} {}", left_, op_name_, right_); }
  bool HasAggregation() const override { return left_->HasAggregation() || right_->HasAggregation(); }

  std::string op_name_;
  std::unique_ptr<Expression> left_;
  std::unique_ptr<Expression> right_;
};

}  // namespace huadb
