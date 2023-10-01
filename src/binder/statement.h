#pragma once

#include <string>

namespace huadb {

enum class StatementType {
  ANALYZE_STATEMENT,
  CHECKPOINT_STATEMENT,
  CREATE_DATABASE_STATEMENT,
  CREATE_INDEX_STATEMENT,
  CREATE_TABLE_STATEMENT,
  DELETE_STATEMENT,
  DROP_DATABASE_STATEMENT,
  DROP_INDEX_STATEMENT,
  DROP_TABLE_STATEMENT,
  EXPLAIN_STATEMENT,
  INSERT_STATEMENT,
  LOCK_STATEMENT,
  SELECT_STATEMENT,
  TRANSACTION_STATEMENT,
  UPDATE_STATEMENT,
  VACUUM_STATEMENT,
  VARIABLE_SET_STATEMENT,
  VARIABLE_SHOW_STATEMENT,
};

class Statement {
 public:
  explicit Statement(StatementType type) : type_(type) {}
  virtual ~Statement() = default;

  virtual std::string ToString() const = 0;

  StatementType type_;
};

}  // namespace huadb
