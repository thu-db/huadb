#pragma once

#include <string>
#include <vector>

#include "common/type_util.h"
#include "common/types.h"
#include "fmt/format.h"

namespace huadb {

// 列的定义，记录列的类型、长度等信息
class ColumnDefinition {
 public:
  ColumnDefinition() = default;
  ColumnDefinition(std::string name, Type type);
  ColumnDefinition(std::string name, Type type, db_size_t max_size);

  // 列名
  const std::string &GetName() const;
  // 列的类型
  Type GetType() const;

  // 列占用的字节数
  db_size_t GetBytes() const;
  // 列的长度
  db_size_t GetMaxSize() const;

  // 字符串格式序列化
  std::string ToString() const;
  std::vector<std::string> ToStringVector() const;

  std::string name_;
  Type type_;
  db_size_t max_size_;
};

}  // namespace huadb

template <typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of<huadb::ColumnDefinition, T>::value, char>>
    : fmt::formatter<std::string> {
  auto format(const T &expr, format_context &ctx) const {
    return fmt::formatter<std::string>::format(expr.ToString(), ctx);
  }
};
