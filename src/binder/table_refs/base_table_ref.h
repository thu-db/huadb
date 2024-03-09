#pragma once

#include <optional>
#include <string>

#include "binder/table_ref.h"
#include "catalog/column_list.h"
#include "common/types.h"
#include "fmt/format.h"

namespace huadb {

class BaseTableRef : public TableRef {
 public:
  BaseTableRef(std::string table, std::optional<std::string> alias, oid_t oid, ColumnList column_list)
      : TableRef(TableRefType::BASE_TABLE),
        alias_(std::move(alias)),
        table_(std::move(table)),
        oid_(oid),
        column_list_(std::move(column_list)) {}
  std::string ToString() const override { return fmt::format("name={}, oid={}", table_, oid_); }

  std::string GetTableName() const {
    if (alias_) {
      return *alias_;
    } else {
      return table_;
    }
  }

  std::string table_;
  std::optional<std::string> alias_;
  oid_t oid_;
  ColumnList column_list_;
};

}  // namespace huadb
