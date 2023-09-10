#pragma once

#include <memory>
#include <vector>

#include "expressions/expression.h"
#include "fmt/ranges.h"
#include "operators/operator.h"
#include "table/record.h"

namespace huadb {

class ProjectionOperator : public Operator {
 public:
  ProjectionOperator(std::shared_ptr<ColumnList> column_list, std::shared_ptr<Operator> child,
                     std::vector<std::shared_ptr<OperatorExpression>> exprs)
      : Operator(OperatorType::PROJECTION, std::move(column_list), {std::move(child)}), exprs_(std::move(exprs)) {}
  std::string ToString(size_t indent_num = 0) const override {
    return fmt::format("{}Projection: {}\n{}", std::string(indent_num * 2, ' '), exprs_,
                       children_[0]->ToString(indent_num + 1));
  }

  std::vector<std::shared_ptr<OperatorExpression>> exprs_;
};

}  // namespace huadb
