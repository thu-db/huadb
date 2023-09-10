#pragma once

#include "binder/statement.h"
#include "fmt/format.h"

namespace huadb {

enum class TransactionType { BEGIN, COMMIT, ROLLBACK };

class TransactionStatement : public Statement {
 public:
  explicit TransactionStatement(TransactionType type) : Statement(StatementType::TRANSACTION_STATEMENT), type_(type) {}
  TransactionType type_;

  std::string ToString() const override { return fmt::format("TransactionStatement: {}\n", type_); }
};

}  // namespace huadb

template <>
struct fmt::formatter<huadb::TransactionType> : formatter<string_view> {
  auto format(huadb::TransactionType type, format_context &ctx) const {
    string_view name = "unknown";
    switch (type) {
      case huadb::TransactionType::BEGIN:
        name = "begin";
        break;
      case huadb::TransactionType::COMMIT:
        name = "commit";
        break;
      case huadb::TransactionType::ROLLBACK:
        name = "rollback";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
