#pragma once

#include "executors/executor.h"
#include "operators/nested_loop_join_operator.h"

namespace huadb {

class NestedLoopJoinExecutor : public Executor {
 public:
  NestedLoopJoinExecutor(ExecutorContext &context, std::shared_ptr<const NestedLoopJoinOperator> plan,
                         std::shared_ptr<Executor> left, std::shared_ptr<Executor> right);
  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const NestedLoopJoinOperator> plan_;
};

}  // namespace huadb
