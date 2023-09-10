#pragma once

#include "expressions/expression.h"
#include "fmt/format.h"
#include "operators/operator.h"

namespace huadb {

class FilterOperator : public Operator {
 public:
  FilterOperator(std::shared_ptr<ColumnList> column_list, std::shared_ptr<Operator> child,
                 std::shared_ptr<OperatorExpression> predicate)
      : Operator(OperatorType::FILTER, std::move(column_list), {std::move(child)}), predicate_(std::move(predicate)) {}
  std::string ToString(size_t indent_num = 0) const override {
    return fmt::format("{}Filter: {}\n{}", std::string(indent_num * 2, ' '), predicate_,
                       children_[0]->ToString(indent_num + 1));
  }

  std::shared_ptr<OperatorExpression> predicate_;
};

}  // namespace huadb
