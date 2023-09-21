#pragma once

#include <optional>

#include "fmt/format.h"
#include "operators/operator.h"
#include "table/table.h"
#include "table/table_scan.h"

namespace huadb {

class SeqScanOperator : public Operator {
 public:
  SeqScanOperator(std::shared_ptr<ColumnList> column_list, oid_t table_oid, std::string table_name,
                  std::optional<std::string> alias, bool has_lock)
      : Operator(OperatorType::SEQSCAN, std::move(column_list), {}),
        table_oid_(table_oid),
        table_name_(std::move(table_name)),
        alias_(std::move(alias)),
        has_lock_(has_lock) {}
  std::string ToString(size_t indent_num = 0) const override {
    if (alias_) {
      return fmt::format("{}SeqScan: {} {}", std::string(indent_num * 2, ' '), table_name_, *alias_);
    } else {
      return fmt::format("{}SeqScan: {}", std::string(indent_num * 2, ' '), table_name_);
    }
  }

  oid_t GetTableOid() const { return table_oid_; }
  const std::string &GetTableNameOrAlias() const {
    if (alias_) {
      return *alias_;
    } else {
      return table_name_;
    }
  }
  const std::string &GetTableName() const { return table_name_; }
  bool HasLock() const { return has_lock_; }

 private:
  oid_t table_oid_;
  std::string table_name_;
  std::optional<std::string> alias_;
  bool has_lock_;
};

}  // namespace huadb
