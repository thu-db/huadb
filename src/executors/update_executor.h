#pragma once

#include "executors/executor.h"
#include "operators/update_operator.h"

namespace huadb {

class UpdateExecutor : public Executor {
 public:
  UpdateExecutor(ExecutorContext &context, std::shared_ptr<const UpdateOperator> plan, std::shared_ptr<Executor> child);

  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const UpdateOperator> plan_;
  std::shared_ptr<Table> table_;
  bool finished_ = false;
};

}  // namespace huadb
