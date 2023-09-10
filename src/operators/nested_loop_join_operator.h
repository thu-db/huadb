#pragma once

#include "binder/table_ref.h"
#include "expressions/expression.h"
#include "fmt/format.h"
#include "operators/operator.h"

namespace huadb {

class Expression;

class NestedLoopJoinOperator : public Operator {
 public:
  NestedLoopJoinOperator(std::shared_ptr<ColumnList> column_list, std::shared_ptr<Operator> left,
                         std::shared_ptr<Operator> right, std::shared_ptr<OperatorExpression> join_condition,
                         JoinType join_type = JoinType::INNER)
      : join_condition_(std::move(join_condition)),
        join_type_(join_type),
        Operator(OperatorType::NESTEDLOOP, std::move(column_list), {std::move(left), std::move(right)}) {}
  std::string ToString(size_t indent_num = 0) const override {
    return fmt::format("{}NestedLoopJoin: {}\n{}\n{}", std::string(indent_num * 2, ' '), join_condition_,
                       children_[0]->ToString(indent_num + 1), children_[1]->ToString(indent_num + 1));
  }
  std::shared_ptr<OperatorExpression> join_condition_;
  JoinType join_type_;
};

}  // namespace huadb
