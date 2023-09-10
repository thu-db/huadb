#pragma once

#include "executors/executor.h"
#include "operators/merge_join_operator.h"

namespace huadb {

class MergeJoinExecutor : public Executor {
 public:
  MergeJoinExecutor(ExecutorContext &context, std::shared_ptr<const MergeJoinOperator> plan,
                    std::shared_ptr<Executor> left, std::shared_ptr<Executor> right);
  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const MergeJoinOperator> plan_;
};

}  // namespace huadb
