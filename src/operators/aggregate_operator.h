#pragma once

#include "expressions/expression.h"
#include "fmt/format.h"
#include "operators/operator.h"

namespace huadb {

enum class AggregateType { AVG, COUNT_STAR, COUNT, SUM, MIN, MAX };

class AggregateOperator : public Operator {
 public:
  AggregateOperator(std::shared_ptr<ColumnList> column_list, std::shared_ptr<Operator> child,
                    std::vector<std::shared_ptr<OperatorExpression>> group_bys,
                    std::vector<std::shared_ptr<OperatorExpression>> aggregates, std::vector<bool> is_distincts,
                    std::vector<AggregateType> aggregate_types)
      : Operator(OperatorType::AGGREGATE, std::move(column_list), {std::move(child)}),
        group_bys_(std::move(group_bys)),
        aggregates_(std::move(aggregates)),
        is_distincts_(std::move(is_distincts)),
        aggregate_types_(std::move(aggregate_types)) {}
  std::string ToString(size_t indent_num = 0) const override {
    return fmt::format("{}Aggregate:\n{}", std::string(indent_num * 2, ' '), children_[0]->ToString(indent_num + 1));
  }

  const std::vector<std::shared_ptr<OperatorExpression>> &GetGroupBys() const { return group_bys_; }
  const std::vector<std::shared_ptr<OperatorExpression>> &GetAggregates() const { return aggregates_; }
  const std::vector<AggregateType> &GetAggregateTypes() const { return aggregate_types_; }

  std::vector<std::shared_ptr<OperatorExpression>> group_bys_;
  std::vector<std::shared_ptr<OperatorExpression>> aggregates_;
  std::vector<bool> is_distincts_;
  std::vector<AggregateType> aggregate_types_;
};

}  // namespace huadb
