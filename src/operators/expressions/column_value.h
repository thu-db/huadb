#pragma once

#include <string>

#include "fmt/format.h"
#include "operators/expressions/expression.h"

namespace huadb {

class ColumnValue : public OperatorExpression {
 public:
  ColumnValue(size_t col_idx, Type col_type, const std::string &name, size_t size, bool is_left = true)
      : col_idx_(col_idx),
        OperatorExpression(OperatorExpressionType::COLUMN_VALUE, {}, col_type, name, size),
        is_left_(is_left) {}
  Value Evaluate(std::shared_ptr<const Record> record) override { return record->GetValue(col_idx_); }
  Value EvaluateJoin(std::shared_ptr<const Record> left, std::shared_ptr<const Record> right) override {
    if (is_left_) {
      return left->GetValue(col_idx_);
    } else {
      return right->GetValue(col_idx_);
    }
  }
  std::string ToString() const override { return fmt::format("{}", name_); }
  size_t GetColumnIndex() const { return col_idx_; }

 private:
  size_t col_idx_;
  bool is_left_;
};

}  // namespace huadb
