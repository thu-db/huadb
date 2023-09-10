#pragma once

#include "fmt/format.h"
#include "operators/expressions/expression.h"
#include "operators/operator.h"
#include "table/record.h"

namespace huadb {

class ValuesOperator : public Operator {
 public:
  ValuesOperator(std::shared_ptr<ColumnList> column_list,
                 std::vector<std::vector<std::shared_ptr<OperatorExpression>>> values)
      : Operator(OperatorType::VALUES, std::move(column_list), {}), values_(std::move(values)) {}
  std::string ToString(size_t indent_num = 0) const override {
    return fmt::format("{}ValuesOperator", std::string(indent_num * 2, ' '));
  }
  std::vector<std::vector<std::shared_ptr<OperatorExpression>>> values_;
};

}  // namespace huadb
