#pragma once

#include <cassert>

#include "binder/table_ref.h"
#include "expressions/expression.h"
#include "fmt/format.h"
#include "operators/operator.h"

namespace huadb {

class HashJoinOperator : public Operator {
 public:
  HashJoinOperator(std::shared_ptr<ColumnList> column_list, std::shared_ptr<Operator> left,
                   std::shared_ptr<Operator> right, std::shared_ptr<OperatorExpression> left_key,
                   std::shared_ptr<OperatorExpression> right_key, JoinType join_type = JoinType::INNER)
      : left_key_(std::move(left_key)),
        right_key_(std::move(right_key)),
        join_type_(join_type),
        Operator(OperatorType::HASHJOIN, std::move(column_list), {std::move(left), std::move(right)}) {}
  std::string ToString(size_t indent_num = 0) const override {
    return fmt::format("{}HashJoin: left={} right={}\n{}\n{}", std::string(indent_num * 2, ' '), left_key_, right_key_,
                       children_[0]->ToString(indent_num + 1), children_[1]->ToString(indent_num + 1));
  }
  std::shared_ptr<OperatorExpression> left_key_;
  std::shared_ptr<OperatorExpression> right_key_;
  JoinType join_type_;
};

}  // namespace huadb
