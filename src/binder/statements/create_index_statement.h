#pragma once

#include "binder/statement.h"
#include "binder/table_refs/base_table_ref.h"

namespace huadb {

class CreateIndexStatement : public Statement {
 public:
  CreateIndexStatement(std::string index_name, std::string table_name, std::vector<std::string> column_names)
      : Statement(StatementType::CREATE_INDEX_STATEMENT),
        index_name_(std::move(index_name)),
        table_name_(std::move(table_name)),
        column_names_(std::move(column_names)) {}
  std::string ToString() const override { return fmt::format("CreateIndexStatement: name={}\n", index_name_); }
  std::string index_name_;
  std::string table_name_;
  std::vector<std::string> column_names_;
};

}  // namespace huadb
