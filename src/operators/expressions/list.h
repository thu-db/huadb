#pragma once

#include "fmt/ranges.h"
#include "operators/expressions/expression.h"

namespace huadb {

class List : public OperatorExpression {
 public:
  explicit List(std::vector<std::shared_ptr<OperatorExpression>> exprs)
      : OperatorExpression(OperatorExpressionType::LIST, {}, Type::LIST, "<no_name>"), exprs_(std::move(exprs)) {}
  Value Evaluate(std::shared_ptr<const Record> record) override {
    std::vector<Value> values;
    for (auto &e : exprs_) {
      values.push_back(e->Evaluate(record));
    }
    return Value(values);
  }
  Value EvaluateJoin(std::shared_ptr<const Record> left, std::shared_ptr<const Record> right) override {
    std::vector<Value> values;
    for (auto &e : exprs_) {
      values.push_back(e->Evaluate(left));
    }
    return Value(values);
  }
  std::string ToString() const override { return fmt::format("{}", exprs_); }
  std::vector<std::shared_ptr<OperatorExpression>> exprs_;
};

}  // namespace huadb
