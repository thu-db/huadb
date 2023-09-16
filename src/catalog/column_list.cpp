#include "catalog/column_list.h"

#include <cassert>
#include <sstream>

#include "common/exceptions.h"

namespace huadb {

ColumnList::ColumnList(const std::vector<ColumnDefinition> &columns) {
  for (const auto &column : columns) {
    col2idx_[column.name_] = col2idx_.size();
    columns_.push_back(column);
  }
}

void ColumnList::AddColumn(ColumnDefinition column) {
  col2idx_[column.name_] = col2idx_.size();
  columns_.push_back(std::move(column));
}

size_t ColumnList::GetColumnIndex(const std::string &name) const {
  auto column_index = TryGetColumnIndex(name);
  if (column_index) {
    return *column_index;
  }
  throw DbException("Column " + name + " not found in column list");
}

std::optional<size_t> ColumnList::TryGetColumnIndex(const std::string &name) const {
  auto entry = col2idx_.find(name);
  if (entry == col2idx_.end()) {
    return std::nullopt;
  }
  return std::optional{entry->second};
}

const std::vector<ColumnDefinition> &ColumnList::GetColumns() const { return columns_; }

const ColumnDefinition &ColumnList::GetColumn(size_t index) const { return columns_[index]; }

size_t ColumnList::Length() const { return columns_.size(); }

size_t ColumnList::Size() const {
  size_t size = sizeof(db_size_t);
  for (const auto &column : columns_) {
    size += column.GetBytes();
  }
  return size;
}

std::string ColumnList::ToString() const {
  std::string str = std::to_string(columns_.size()) + "\n";
  for (const auto &column : columns_) {
    str += column.ToString() + "\n";
  }
  return str;
}

void ColumnList::FromString(const std::string &str) {
  std::istringstream iss(str);
  db_size_t size;
  iss >> size;
  columns_.resize(size);
  for (auto &column : columns_) {
    iss >> column.name_;
    std::string type;
    iss >> type;
    column.type_ = TypeUtil::String2Type(type);
    iss >> column.max_size_;
  }
  for (auto i = 0; i < columns_.size(); ++i) {
    col2idx_[columns_[i].name_] = i;
  }
}

}  // namespace huadb
