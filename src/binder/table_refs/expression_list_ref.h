#pragma once

#include <memory>
#include <vector>

#include "binder/table_ref.h"
#include "fmt/ranges.h"

namespace huadb {

class Expression;

class ExpressionListRef : public TableRef {
 public:
  explicit ExpressionListRef(std::vector<std::vector<std::unique_ptr<Expression>>> values_list)
      : TableRef(TableRefType::EXPRESSION_LIST), values_list_(std::move(values_list)) {}
  std::string ToString() const override { return fmt::format("ExpressionListRef: values="); }

  std::vector<std::vector<std::unique_ptr<Expression>>> values_list_;
};

}  // namespace huadb
