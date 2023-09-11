#pragma once

#include "binder/statement.h"
#include "binder/table_refs/base_table_ref.h"
#include "fmt/format.h"

namespace huadb {

enum class TableLockType { SHARE, EXCLUSIVE };

class LockStatement : public Statement {
 public:
  explicit LockStatement(std::unique_ptr<BaseTableRef> table, TableLockType lock_type)
      : Statement(StatementType::LOCK_STATEMENT), table_(std::move(table)), lock_type_(lock_type) {}
  std::unique_ptr<BaseTableRef> table_;
  TableLockType lock_type_;

  std::string ToString() const override { return fmt::format("LockStatement: {} {}\n", table_, lock_type_); }
};

}  // namespace huadb

template <>
struct fmt::formatter<huadb::TableLockType> : formatter<string_view> {
  auto format(huadb::TableLockType type, format_context &ctx) const {
    string_view name = "unknown";
    switch (type) {
      case huadb::TableLockType::SHARE:
        name = "share";
        break;
      case huadb::TableLockType::EXCLUSIVE:
        name = "exclusive";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
