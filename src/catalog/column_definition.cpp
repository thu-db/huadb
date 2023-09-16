#include "catalog/column_definition.h"

#include <cassert>

namespace huadb {

ColumnDefinition::ColumnDefinition(std::string name, Type type)
    : name_(std::move(name)), type_(type), max_size_(TypeUtil::TypeSize(type)) {}

ColumnDefinition::ColumnDefinition(std::string name, Type type, db_size_t max_size)
    : name_(std::move(name)), type_(type), max_size_(max_size) {}

const std::string &ColumnDefinition::GetName() const { return name_; }

Type ColumnDefinition::GetType() const { return type_; }

db_size_t ColumnDefinition::GetBytes() const {
  return sizeof(db_size_t) + name_.size() + sizeof(type_) + sizeof(max_size_);
}

db_size_t ColumnDefinition::GetMaxSize() const { return max_size_; }

std::string ColumnDefinition::ToString() const {
  return name_ + " " + TypeUtil::Type2String(type_) + " " + std::to_string(max_size_);
}

std::vector<std::string> ColumnDefinition::ToStringVector() const {
  std::vector<std::string> str_vec;
  str_vec.emplace_back(name_);
  str_vec.emplace_back(TypeUtil::Type2String(type_));
  str_vec.emplace_back(std::to_string(max_size_));
  return str_vec;
}

}  // namespace huadb
