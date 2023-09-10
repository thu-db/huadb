#pragma once

#include "executors/executor.h"
#include "operators/seqscan_operator.h"

namespace huadb {

class SeqScanExecutor : public Executor {
 public:
  SeqScanExecutor(ExecutorContext &context, std::shared_ptr<const SeqScanOperator> plan);

  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const SeqScanOperator> plan_;
  std::unique_ptr<TableScan> scan_;
};

}  // namespace huadb
