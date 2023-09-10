#pragma once

#include <string>

#include "common/exceptions.h"
#include "fmt/format.h"

namespace huadb {

enum class TableRefType { BASE_TABLE, CROSS_JOIN, EMPTY, EXPRESSION_LIST, JOIN };

enum class JoinType { INNER, LEFT, RIGHT, FULL };

class TableRef {
 public:
  explicit TableRef(TableRefType type) : type_(type) {}
  virtual ~TableRef() = default;
  virtual std::string ToString() const {
    switch (type_) {
      case TableRefType::EMPTY:
        return "<empty>";
      default:
        throw DbException("Unimplemented ToString in TableRef");
    }
  }
  TableRefType type_;
};

}  // namespace huadb

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<huadb::TableRef, T>::value, char>>
    : fmt::formatter<std::string> {
  auto format(const std::unique_ptr<T> &expr, format_context &ctx) const {
    if (!expr) {
      return fmt::formatter<std::string>::format("", ctx);
    }
    return fmt::formatter<std::string>::format(expr->ToString(), ctx);
  }
};

template <>
struct fmt::formatter<huadb::JoinType> : formatter<string_view> {
  auto format(huadb::JoinType type, format_context &ctx) const {
    string_view name = "unknown";
    switch (type) {
      case huadb::JoinType::INNER:
        name = "inner";
        break;
      case huadb::JoinType::LEFT:
        name = "left";
        break;
      case huadb::JoinType::RIGHT:
        name = "right";
        break;
      case huadb::JoinType::FULL:
        name = "full";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
