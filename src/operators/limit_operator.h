#pragma once

#include <optional>

#include "fmt/format.h"
#include "operators/operator.h"

namespace huadb {

class LimitOperator : public Operator {
 public:
  LimitOperator(std::shared_ptr<ColumnList> column_list, std::shared_ptr<Operator> child,
                std::optional<uint32_t> limit_count, std::optional<uint32_t> limit_offset)
      : Operator(OperatorType::LIMIT, std::move(column_list), {std::move(child)}),
        limit_count_(limit_count),
        limit_offset_(limit_offset) {}
  std::string ToString(size_t indent_num = 0) const override {
    return fmt::format("{}LimitOperator:\n{}", std::string(indent_num * 2, ' '),
                       children_[0]->ToString(indent_num + 1));
  }

  std::optional<uint32_t> limit_count_;
  std::optional<uint32_t> limit_offset_;
};

}  // namespace huadb
