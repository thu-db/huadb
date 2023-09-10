#pragma once

#include "executors/executor.h"
#include "operators/lock_rows_operator.h"

namespace huadb {

class LockRowsExecutor : public Executor {
 public:
  LockRowsExecutor(ExecutorContext &context, std::shared_ptr<const LockRowsOperator> plan,
                   std::shared_ptr<Executor> child);
  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const LockRowsOperator> plan_;
};

}  // namespace huadb
