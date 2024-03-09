#pragma once

#include "common/types.h"
#include "fmt/format.h"
#include "operators/operator.h"
#include "table/table.h"

namespace huadb {

class DeleteOperator : public Operator {
 public:
  DeleteOperator(std::shared_ptr<ColumnList> column_list, std::shared_ptr<Operator> child, oid_t oid)
      : Operator(OperatorType::DELETE, std::move(column_list), {std::move(child)}), oid_(oid) {}
  std::string ToString(size_t indent_num = 0) const override {
    return fmt::format("{}DeleteOperator:\n{}", std::string(indent_num * 2, ' '),
                       children_[0]->ToString(indent_num + 1));
  }

  oid_t GetTableOid() const { return oid_; }

 private:
  oid_t oid_;
};

}  // namespace huadb
