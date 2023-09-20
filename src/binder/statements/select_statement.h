#pragma once

#include <vector>

#include "binder/expression.h"
#include "binder/order_by.h"
#include "binder/statement.h"
#include "binder/table_ref.h"
#include "fmt/ranges.h"

namespace huadb {

enum class SelectLockType { NOLOCK, SHARE, UPDATE };

class SelectStatement : public Statement {
 public:
  // Used for Insert Binder
  SelectStatement(std::unique_ptr<TableRef> table, std::vector<std::unique_ptr<Expression>> select_list)
      : Statement(StatementType::SELECT_STATEMENT), table_(std::move(table)), select_list_(std::move(select_list)) {}

  SelectStatement(std::unique_ptr<TableRef> table, std::vector<std::unique_ptr<Expression>> select_list,
                  std::unique_ptr<Expression> where, std::vector<std::unique_ptr<Expression>> group_by,
                  std::unique_ptr<Expression> having, std::vector<std::unique_ptr<OrderBy>> order_by,
                  std::unique_ptr<Expression> limit_count, std::unique_ptr<Expression> limit_offset,
                  SelectLockType lock_type, bool distinct)
      : Statement(StatementType::SELECT_STATEMENT),
        table_(std::move(table)),
        select_list_(std::move(select_list)),
        where_(std::move(where)),
        group_by_(std::move(group_by)),
        having_(std::move(having)),
        order_by_(std::move(order_by)),
        limit_count_(std::move(limit_count)),
        limit_offset_(std::move(limit_offset)),
        lock_type_(lock_type),
        distinct_(distinct) {}
  std::string ToString() const override {
    return fmt::format(
        "SelectStatement:\n  table: {},\n  select_list: {},\n  where: {},\n  order_by: {},\n  limit: {},\n  offset: "
        "{},\n  "
        "distinct: {}\n",
        table_, select_list_, where_, order_by_, limit_count_, limit_offset_, distinct_);
  }
  std::unique_ptr<TableRef> table_;
  std::vector<std::unique_ptr<Expression>> select_list_;
  std::unique_ptr<Expression> where_;
  std::vector<std::unique_ptr<Expression>> group_by_;
  std::unique_ptr<Expression> having_;
  std::vector<std::unique_ptr<OrderBy>> order_by_;
  std::unique_ptr<Expression> limit_count_;
  std::unique_ptr<Expression> limit_offset_;
  SelectLockType lock_type_ = SelectLockType::NOLOCK;
  bool distinct_ = false;
};

}  // namespace huadb
