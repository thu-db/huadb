#pragma once

#include "expressions/expression.h"
#include "fmt/format.h"
#include "operators/operator.h"
#include "table/table.h"

namespace huadb {

class UpdateOperator : public Operator {
 public:
  UpdateOperator(std::shared_ptr<ColumnList> column_list, std::shared_ptr<Operator> child, oid_t oid,
                 std::vector<std::shared_ptr<OperatorExpression>> update_exprs)
      : Operator(OperatorType::UPDATE, std::move(column_list), {std::move(child)}),
        oid_(oid),
        update_exprs_(std::move(update_exprs)) {}
  std::string ToString(size_t indent_num = 0) const override {
    return fmt::format("{}UpdateOperator:\n{}", std::string(indent_num * 2, ' '),
                       children_[0]->ToString(indent_num + 1));
  }

  oid_t GetTableOid() const { return oid_; }

  oid_t oid_;
  std::vector<std::shared_ptr<OperatorExpression>> update_exprs_;
};

}  // namespace huadb
