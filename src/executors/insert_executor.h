#pragma once

#include "executors/executor.h"
#include "operators/insert_operator.h"
#include "table/table.h"

namespace huadb {

class InsertExecutor : public Executor {
 public:
  InsertExecutor(ExecutorContext &context, std::shared_ptr<const InsertOperator> plan, std::shared_ptr<Executor> child);
  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const InsertOperator> plan_;
  std::shared_ptr<Table> table_;
  ColumnList column_list_;
  bool finished_ = false;
};

}  // namespace huadb
