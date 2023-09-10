#pragma once

#include <string>
#include <vector>

#include "binder/expression.h"
#include "fmt/format.h"

namespace huadb {

class ColumnRefExpression : public Expression {
 public:
  ColumnRefExpression() = default;
  explicit ColumnRefExpression(std::vector<std::string> col_name)
      : Expression(ExpressionType::COLUMN_REF), col_name_(std::move(col_name)) {}
  static std::unique_ptr<ColumnRefExpression> Prepend(std::unique_ptr<ColumnRefExpression> column_ref,
                                                      std::string prefix) {
    std::vector<std::string> col_name{std::move(prefix)};
    for (auto &name : column_ref->col_name_) {
      col_name.push_back(std::move(name));
    }
    return std::make_unique<ColumnRefExpression>(std::move(col_name));
  }
  std::string ToString() const override { return fmt::format("{}", fmt::join(col_name_, ".")); }
  bool HasAggregation() const override { return false; }
  std::vector<std::string> col_name_;
};

}  // namespace huadb
