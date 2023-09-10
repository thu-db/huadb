#pragma once

#include "executors/executor.h"
#include "operators/values_operator.h"

namespace huadb {

class ValuesExecutor : public Executor {
 public:
  ValuesExecutor(ExecutorContext &context, std::shared_ptr<const ValuesOperator> plan);

  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const ValuesOperator> plan_;
  size_t cursor_ = 0;
};

}  // namespace huadb
