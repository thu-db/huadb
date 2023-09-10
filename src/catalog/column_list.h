#pragma once

#include <optional>
#include <unordered_map>
#include <vector>

#include "catalog/column_definition.h"

namespace huadb {

class ColumnList {
 public:
  ColumnList() = default;
  explicit ColumnList(const std::vector<ColumnDefinition> &columns);

  // 添加列
  void AddColumn(ColumnDefinition column);
  // 根据列名获取列的位置
  size_t GetColumnIndex(const std::string &name) const;
  std::optional<size_t> TryGetColumnIndex(const std::string &name) const;
  // 获取所有列
  const std::vector<ColumnDefinition> &GetColumns() const;
  // 根据下标获取列
  const ColumnDefinition &GetColumn(size_t index) const;

  // 获取列的数量
  size_t Length() const;
  // 获取总空间占用量
  size_t Size() const;

  // 字符串格式序列化
  std::string ToString() const;
  // 字符串格式反序列化
  void FromString(const std::string &str);

 private:
  std::vector<ColumnDefinition> columns_;
  // 列名到列索引的映射表
  std::unordered_map<std::string, size_t> col2idx_;
};

}  // namespace huadb
