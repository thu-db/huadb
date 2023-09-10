#pragma once

#include "binder/order_by.h"
#include "expressions/expression.h"
#include "fmt/format.h"
#include "operators/operator.h"

namespace huadb {

class OrderByOperator : public Operator {
 public:
  OrderByOperator(std::shared_ptr<ColumnList> column_list, std::shared_ptr<Operator> child,
                  std::vector<std::pair<OrderByType, std::shared_ptr<OperatorExpression>>> order_bys)
      : Operator(OperatorType::ORDERBY, std::move(column_list), {std::move(child)}), order_bys_(std::move(order_bys)) {}
  std::string ToString(size_t indent_num = 0) const override {
    return fmt::format("{}Order:\n{}", std::string(indent_num * 2, ' '), children_[0]->ToString(indent_num + 1));
  }

  std::vector<std::pair<OrderByType, std::shared_ptr<OperatorExpression>>> order_bys_;
};

}  // namespace huadb
