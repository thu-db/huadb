#include "executors/hash_join_executor.h"

namespace huadb {

HashJoinExecutor::HashJoinExecutor(ExecutorContext &context, std::shared_ptr<const HashJoinOperator> plan,
                                   std::shared_ptr<Executor> left, std::shared_ptr<Executor> right)
    : Executor(context, {std::move(left), std::move(right)}), plan_(std::move(plan)) {}

void HashJoinExecutor::Init() {
  children_[0]->Init();
  children_[1]->Init();
  // LAB 4 ADVANCED BEGIN
}

std::shared_ptr<Record> HashJoinExecutor::Next() {
  // LAB 4 ADVANCED BEGIN
  return nullptr;
}

}  // namespace huadb
