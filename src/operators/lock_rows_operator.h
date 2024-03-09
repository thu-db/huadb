#pragma once

#include "binder/statements/select_statement.h"
#include "common/types.h"
#include "fmt/format.h"
#include "operators/operator.h"

namespace huadb {

class LockRowsOperator : public Operator {
 public:
  LockRowsOperator(std::shared_ptr<ColumnList> column_list, std::shared_ptr<Operator> child, oid_t oid,
                   SelectLockType lock_type)
      : Operator(OperatorType::LOCK_ROWS, std::move(column_list), {std::move(child)}),
        oid_(oid),
        lock_type_(lock_type) {}
  std::string ToString(size_t indent_num = 0) const override {
    return fmt::format("{}LockRowsOperator:\n{}", std::string(indent_num * 2, ' '),
                       children_[0]->ToString(indent_num + 1));
  }

  oid_t GetOid() const { return oid_; }
  SelectLockType GetLockType() const { return lock_type_; }

 private:
  oid_t oid_;
  SelectLockType lock_type_;
};

}  // namespace huadb
