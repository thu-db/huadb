#pragma once

#include "common/types.h"
#include "fmt/format.h"
#include "operators/operator.h"

namespace huadb {

class InsertOperator : public Operator {
 public:
  InsertOperator(std::shared_ptr<ColumnList> column_list, std::shared_ptr<Operator> child, ColumnList insert_columns,
                 oid_t oid)
      : Operator(OperatorType::INSERT, std::move(column_list), {std::move(child)}),
        insert_columns_(std::move(insert_columns)),
        oid_(oid) {}
  std::string ToString(size_t indent_num = 0) const override {
    return fmt::format("{}InsertOperator\n{}", std::string(indent_num * 2, ' '),
                       children_[0]->ToString(indent_num + 1));
  }

  oid_t GetTableOid() const { return oid_; }
  const ColumnList &GetInsertColumns() const { return insert_columns_; }

 private:
  oid_t oid_;
  ColumnList insert_columns_;
};

}  // namespace huadb
