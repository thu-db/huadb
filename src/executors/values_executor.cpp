#include "executors/values_executor.h"

namespace huadb {

ValuesExecutor::ValuesExecutor(ExecutorContext &context, std::shared_ptr<const ValuesOperator> plan)
    : Executor(context, {}), plan_(std::move(plan)) {}

void ValuesExecutor::Init() {}

std::shared_ptr<Record> ValuesExecutor::Next() {
  if (cursor_ >= plan_->values_.size()) {
    return nullptr;
  }
  std::vector<Value> values;
  for (const auto &expr : plan_->values_[cursor_]) {
    values.push_back(expr->Evaluate(nullptr));
  }
  cursor_++;
  return std::make_unique<Record>(std::move(values));
}

}  // namespace huadb
