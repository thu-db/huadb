#pragma once

#include <string>

#include "common/exceptions.h"
#include "fmt/format.h"

namespace huadb {

enum class ExpressionType {
  AGGREGATE,
  ALIAS,
  BINARY_OP,
  TYPE_CAST,
  COLUMN_REF,
  CONST,
  FUNC_CALL,
  INVALID,
  LIST,
  NULL_TEST,
  STAR,
  UNARY_OP,
};

class Expression {
 public:
  Expression() : type_(ExpressionType::INVALID) {}
  explicit Expression(ExpressionType type) : type_(type) {}
  virtual ~Expression() = default;

  virtual std::string ToString() const { return ""; }
  virtual bool HasAggregation() const { throw DbException("HasAggregation not implemented"); }
  ExpressionType type_;
};

}  // namespace huadb

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<huadb::Expression, T>::value, char>>
    : fmt::formatter<std::string> {
  auto format(const std::unique_ptr<T> &expr, format_context &ctx) const {
    if (!expr) {
      return fmt::formatter<std::string>::format("", ctx);
    }
    return fmt::formatter<std::string>::format(expr->ToString(), ctx);
  }
};

template <typename T>
struct fmt::formatter<std::shared_ptr<T>, std::enable_if_t<std::is_base_of<huadb::Expression, T>::value, char>>
    : fmt::formatter<std::string> {
  auto format(const std::shared_ptr<T> &expr, format_context &ctx) const {
    if (!expr) {
      return fmt::formatter<std::string>::format("", ctx);
    }
    return fmt::formatter<std::string>::format(expr->ToString(), ctx);
  }
};
