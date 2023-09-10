#pragma once

#include "executors/executor.h"
#include "operators/delete_operator.h"

namespace huadb {

class DeleteExecutor : public Executor {
 public:
  DeleteExecutor(ExecutorContext &context, std::shared_ptr<const DeleteOperator> plan, std::shared_ptr<Executor> child);

  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const DeleteOperator> plan_;
  std::shared_ptr<Table> table_;
  bool finished_ = false;
};

}  // namespace huadb
