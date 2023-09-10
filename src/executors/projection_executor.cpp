#include "executors/projection_executor.h"

namespace huadb {

ProjectionExecutor::ProjectionExecutor(ExecutorContext &context, std::shared_ptr<const ProjectionOperator> plan,
                                       std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void ProjectionExecutor::Init() { children_[0]->Init(); }

std::shared_ptr<Record> ProjectionExecutor::Next() {
  auto record = children_[0]->Next();
  if (!record) {
    return nullptr;
  }
  std::vector<Value> values;
  for (const auto &expr : plan_->exprs_) {
    values.push_back(expr->Evaluate(record));
  }
  return std::make_unique<Record>(std::move(values), record->GetRid());
}

}  // namespace huadb
