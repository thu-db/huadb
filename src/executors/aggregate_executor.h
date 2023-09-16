#pragma once

#include "executors/executor.h"
#include "operators/aggregate_operator.h"

namespace huadb {

class AggregateExecutor : public Executor {
 public:
  AggregateExecutor(ExecutorContext &context, std::shared_ptr<const AggregateOperator> plan,
                    std::shared_ptr<Executor> child);
  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const AggregateOperator> plan_;
};

}  // namespace huadb
