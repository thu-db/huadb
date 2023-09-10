#include "executors/aggregate_executor.h"

namespace huadb {

AggregateExecutor::AggregateExecutor(ExecutorContext &context, std::shared_ptr<const AggregateOperator> plan,
                                     std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {
  // LAB 4 ADVANCED BEGIN
}

void AggregateExecutor::Init() {
  // LAB 4 ADVANCED BEGIN
}

std::shared_ptr<Record> AggregateExecutor::Next() {
  // LAB 4 ADVANCED BEGIN
  return nullptr;
}

}  // namespace huadb
