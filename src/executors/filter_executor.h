#pragma once

#include "executors/executor.h"
#include "operators/filter_operator.h"

namespace huadb {

class FilterExecutor : public Executor {
 public:
  FilterExecutor(ExecutorContext &context, std::shared_ptr<const FilterOperator> plan, std::shared_ptr<Executor> child);
  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const FilterOperator> plan_;
  std::shared_ptr<Table> table_;
};

}  // namespace huadb
