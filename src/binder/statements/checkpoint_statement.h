#pragma once

#include "binder/statement.h"

namespace huadb {

class CheckpointStatement : public Statement {
 public:
  CheckpointStatement() : Statement(StatementType::CHECKPOINT_STATEMENT) {}
  std::string ToString() const override { return "CheckpointStatement\n"; }
};

}  // namespace huadb
