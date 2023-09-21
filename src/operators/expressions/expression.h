#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/exceptions.h"
#include "common/value.h"
#include "fmt/format.h"
#include "table/record.h"

namespace huadb {

enum class OperatorExpressionType {
  AGGREGATE,
  ARITHMETIC,
  TYPE_CAST,
  COLUMN_VALUE,
  COMPARISON,
  CONST,
  FUNC_CALL,
  LIST,
  LOGIC,
  NULL_TEST
};

class OperatorExpression {
 public:
  OperatorExpression() = default;
  OperatorExpression(OperatorExpressionType expr_type, std::vector<std::shared_ptr<OperatorExpression>> children,
                     Type value_type, std::string name = "<no_name>", size_t size = 0)
      : expr_type_(expr_type),
        children_(std::move(children)),
        value_type_(value_type),
        name_(std::move(name)),
        size_(size) {}
  virtual Value Evaluate(std::shared_ptr<const Record> record) { throw DbException("Evaluate method not implemented"); }
  virtual Value EvaluateJoin(std::shared_ptr<const Record> left, std::shared_ptr<const Record> right) {
    throw DbException("EvaluateJoin method not implemented");
  }
  virtual std::string ToString() const { return "OperatorExpression"; }

  OperatorExpressionType GetExprType() const { return expr_type_; }
  Type GetValueType() const { return value_type_; }
  size_t GetSize() const { return size_; }
  void SetName(const std::string &name) { name_ = name; }

  OperatorExpressionType expr_type_;
  std::string name_;
  std::vector<std::shared_ptr<OperatorExpression>> children_;
  Type value_type_;
  size_t size_;
};

}  // namespace huadb

template <typename T>
struct fmt::formatter<std::shared_ptr<T>, std::enable_if_t<std::is_base_of<huadb::OperatorExpression, T>::value, char>>
    : fmt::formatter<std::string> {
  auto format(const std::shared_ptr<T> &expr, format_context &ctx) const {
    if (!expr) {
      return fmt::formatter<std::string>::format("", ctx);
    }
    return fmt::formatter<std::string>::format(expr->ToString(), ctx);
  }
};
