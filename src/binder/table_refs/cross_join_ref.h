#pragma once

#include <memory>

#include "binder/table_ref.h"
#include "fmt/format.h"

namespace huadb {

class CrossJoinRef : public TableRef {
 public:
  CrossJoinRef(std::unique_ptr<TableRef> left, std::unique_ptr<TableRef> right)
      : TableRef(TableRefType::CROSS_JOIN), left_(std::move(left)), right_(std::move(right)) {}
  std::string ToString() const override { return fmt::format("left=({}), right=({})", left_, right_); }

  std::unique_ptr<TableRef> left_;
  std::unique_ptr<TableRef> right_;
};

}  // namespace huadb
