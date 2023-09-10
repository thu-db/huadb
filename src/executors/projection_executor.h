#pragma once

#include "executors/executor.h"
#include "operators/projection_operator.h"

namespace huadb {

class ProjectionExecutor : public Executor {
 public:
  ProjectionExecutor(ExecutorContext &context, std::shared_ptr<const ProjectionOperator> plan,
                     std::shared_ptr<Executor> child);
  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const ProjectionOperator> plan_;
};

}  // namespace huadb
