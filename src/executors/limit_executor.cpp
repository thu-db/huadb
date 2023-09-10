#include "executors/limit_executor.h"

namespace huadb {

LimitExecutor::LimitExecutor(ExecutorContext &context, std::shared_ptr<const LimitOperator> plan,
                             std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void LimitExecutor::Init() { children_[0]->Init(); }

std::shared_ptr<Record> LimitExecutor::Next() {
  // 通过 plan_ 获取 limit 语句中的 offset 和 limit 值
  // LAB 4 BEGIN
  return children_[0]->Next();
}

}  // namespace huadb
