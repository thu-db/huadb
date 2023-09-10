#pragma once

#include <memory>
#include <vector>

#include "catalog/column_list.h"

namespace huadb {

enum class OperatorType {
  AGGREGATE,
  DELETE,
  FILTER,
  HASHJOIN,
  INSERT,
  LIMIT,
  LOCK_ROWS,
  MERGEJOIN,
  NESTEDLOOP,
  ORDERBY,
  PROJECTION,
  SEQSCAN,
  UPDATE,
  VALUES,
};

class Operator {
 public:
  Operator(OperatorType type, std::shared_ptr<ColumnList> column_list, std::vector<std::shared_ptr<Operator>> children)
      : type_(type), column_list_(std::move(column_list)), children_(std::move(children)) {}
  virtual ~Operator() = default;
  virtual std::string ToString(size_t indent_num = 0) const = 0;

  const ColumnList &OutputColumns() const { return *column_list_; }
  OperatorType GetType() const { return type_; }
  const std::vector<std::shared_ptr<Operator>> &GetChildren() const { return children_; }

  OperatorType type_;
  std::shared_ptr<ColumnList> column_list_;
  std::vector<std::shared_ptr<Operator>> children_;
};

}  // namespace huadb
