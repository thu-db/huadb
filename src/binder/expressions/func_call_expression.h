#pragma once

#include "binder/expression.h"
#include "fmt/ranges.h"

namespace huadb {

class FuncCallExpression : public Expression {
 public:
  FuncCallExpression(std::string function_name, std::vector<std::unique_ptr<Expression>> args)
      : Expression(ExpressionType::FUNC_CALL), function_name_(std::move(function_name)), args_(std::move(args)) {}
  std::string ToString() const override { return fmt::format("{}({})", function_name_, args_); }
  bool HasAggregation() const override {
    for (const auto &arg : args_) {
      if (arg->HasAggregation()) {
        return true;
      }
    }
    return false;
  }

  std::string function_name_;
  std::vector<std::unique_ptr<Expression>> args_;
};

}  // namespace huadb
