#include "executors/filter_executor.h"

namespace huadb {

FilterExecutor::FilterExecutor(ExecutorContext &context, std::shared_ptr<const FilterOperator> plan,
                               std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void FilterExecutor::Init() { children_[0]->Init(); }

std::shared_ptr<Record> FilterExecutor::Next() {
  while (auto record = children_[0]->Next()) {
    auto value = plan_->predicate_->Evaluate(record);
    if (!value.IsNull() && value.GetValue<bool>()) {
      return record;
    }
  }
  return nullptr;
}

}  // namespace huadb
