#pragma once

#include <memory>

#include "binder/expression.h"
#include "fmt/format.h"

namespace huadb {

enum class OrderByType { DEFAULT, ASC, DESC };

class OrderBy {
 public:
  OrderBy(OrderByType type, std::unique_ptr<Expression> expr) : type_(type), expr_(std::move(expr)) {}
  std::string ToString() const { return fmt::format("(type={}, expr={})", type_, expr_->ToString()); }

  OrderByType type_;
  std::unique_ptr<Expression> expr_;
};

}  // namespace huadb

template <>
struct fmt::formatter<huadb::OrderByType> : formatter<string_view> {
  auto format(huadb::OrderByType type, format_context &ctx) const {
    string_view name = "unknown";
    switch (type) {
      case huadb::OrderByType::DEFAULT:
        name = "Default";
        break;
      case huadb::OrderByType::ASC:
        name = "Ascending";
        break;
      case huadb::OrderByType::DESC:
        name = "Descending";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<huadb::OrderBy, T>::value, char>>
    : fmt::formatter<std::string> {
  auto format(const std::unique_ptr<T> &expr, format_context &ctx) const {
    if (!expr) {
      return fmt::formatter<std::string>::format("", ctx);
    }
    return fmt::formatter<std::string>::format(expr->ToString(), ctx);
  }
};
