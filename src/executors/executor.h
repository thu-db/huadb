#pragma once

#include "executors/executor_context.h"
#include "table/record.h"

namespace huadb {

class Executor {
 public:
  explicit Executor(ExecutorContext &context, std::vector<std::shared_ptr<Executor>> children)
      : context_(context), children_(std::move(children)) {}
  virtual ~Executor() = default;

  virtual void Init() = 0;
  virtual std::shared_ptr<Record> Next() = 0;

 protected:
  ExecutorContext &context_;
  std::vector<std::shared_ptr<Executor>> children_;
};

}  // namespace huadb
