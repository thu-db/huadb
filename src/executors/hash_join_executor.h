#pragma once

#include <unordered_map>

#include "executors/executor.h"
#include "operators/hash_join_operator.h"

namespace huadb {

class HashJoinExecutor : public Executor {
 public:
  HashJoinExecutor(ExecutorContext &context, std::shared_ptr<const HashJoinOperator> plan,
                   std::shared_ptr<Executor> left, std::shared_ptr<Executor> right);

  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const HashJoinOperator> plan_;
};

}  // namespace huadb
