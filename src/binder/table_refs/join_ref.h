#pragma once

#include "binder/table_ref.h"

namespace huadb {

class Expression;

class JoinRef : public TableRef {
 public:
  JoinRef(JoinType join_type, std::unique_ptr<TableRef> left, std::unique_ptr<TableRef> right,
          std::unique_ptr<Expression> condition)
      : TableRef(TableRefType::JOIN),
        join_type_(join_type),
        left_(std::move(left)),
        right_(std::move(right)),
        condition_(std::move(condition)) {}
  std::string ToString() const override {
    return fmt::format("left=({}), right=({}), type={}, condition={}", left_, right_, join_type_, condition_);
  }

  JoinType join_type_;
  std::unique_ptr<TableRef> left_;
  std::unique_ptr<TableRef> right_;

  std::unique_ptr<Expression> left_key_;
  std::unique_ptr<Expression> right_key_;
  std::unique_ptr<Expression> condition_;
};

}  // namespace huadb
