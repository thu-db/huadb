#pragma once

#include <memory>

#include "executors/aggregate_executor.h"
#include "executors/delete_executor.h"
#include "executors/executor.h"
#include "executors/executor_context.h"
#include "executors/executor_factory.h"
#include "executors/filter_executor.h"
#include "executors/hash_join_executor.h"
#include "executors/insert_executor.h"
#include "executors/limit_executor.h"
#include "executors/lock_rows_executor.h"
#include "executors/merge_join_executor.h"
#include "executors/nested_loop_join_executor.h"
#include "executors/orderby_executor.h"
#include "executors/projection_executor.h"
#include "executors/seqscan_executor.h"
#include "executors/update_executor.h"
#include "executors/values_executor.h"

namespace huadb {

class ExecutorFactory {
 public:
  static std::unique_ptr<Executor> CreateExecutor(ExecutorContext &context, std::shared_ptr<const Operator> plan) {
    switch (plan->GetType()) {
      case OperatorType::SEQSCAN: {
        auto seqscan_operator = std::dynamic_pointer_cast<const SeqScanOperator>(plan);
        return std::make_unique<SeqScanExecutor>(context, std::move(seqscan_operator));
      }
      case OperatorType::INSERT: {
        auto insert_operator = std::dynamic_pointer_cast<const InsertOperator>(plan);
        auto child = CreateExecutor(context, plan->GetChildren()[0]);
        return std::make_unique<InsertExecutor>(context, std::move(insert_operator), std::move(child));
      }
      case OperatorType::DELETE: {
        auto delete_operator = std::dynamic_pointer_cast<const DeleteOperator>(plan);
        auto child = CreateExecutor(context, plan->GetChildren()[0]);
        return std::make_unique<DeleteExecutor>(context, std::move(delete_operator), std::move(child));
      }
      case OperatorType::UPDATE: {
        auto update_operator = std::dynamic_pointer_cast<const UpdateOperator>(plan);
        auto child = CreateExecutor(context, plan->GetChildren()[0]);
        return std::make_unique<UpdateExecutor>(context, std::move(update_operator), std::move(child));
      }
      case OperatorType::PROJECTION: {
        auto projection_operator = std::dynamic_pointer_cast<const ProjectionOperator>(plan);
        auto child = CreateExecutor(context, plan->GetChildren()[0]);
        return std::make_unique<ProjectionExecutor>(context, std::move(projection_operator), std::move(child));
      }
      case OperatorType::VALUES: {
        auto values_operator = std::dynamic_pointer_cast<const ValuesOperator>(plan);
        return std::make_unique<ValuesExecutor>(context, std::move(values_operator));
      }
      case OperatorType::NESTEDLOOP: {
        auto nested_loop_operator = std::dynamic_pointer_cast<const NestedLoopJoinOperator>(plan);
        auto left = CreateExecutor(context, plan->GetChildren()[0]);
        auto right = CreateExecutor(context, plan->GetChildren()[1]);
        return std::make_unique<NestedLoopJoinExecutor>(context, std::move(nested_loop_operator), std::move(left),
                                                        std::move(right));
      }
      case OperatorType::MERGEJOIN: {
        auto merge_join_operator = std::dynamic_pointer_cast<const MergeJoinOperator>(plan);
        auto left = CreateExecutor(context, plan->GetChildren()[0]);
        auto right = CreateExecutor(context, plan->GetChildren()[1]);
        return std::make_unique<MergeJoinExecutor>(context, std::move(merge_join_operator), std::move(left),
                                                   std::move(right));
      }
      case OperatorType::HASHJOIN: {
        auto hash_join_operator = std::dynamic_pointer_cast<const HashJoinOperator>(plan);
        auto left = CreateExecutor(context, plan->GetChildren()[0]);
        auto right = CreateExecutor(context, plan->GetChildren()[1]);
        return std::make_unique<HashJoinExecutor>(context, std::move(hash_join_operator), std::move(left),
                                                  std::move(right));
      }
      case OperatorType::FILTER: {
        auto filter_operator = std::dynamic_pointer_cast<const FilterOperator>(plan);
        auto child = CreateExecutor(context, plan->GetChildren()[0]);
        return std::make_unique<FilterExecutor>(context, std::move(filter_operator), std::move(child));
      }
      case OperatorType::LIMIT: {
        auto limit_operator = std::dynamic_pointer_cast<const LimitOperator>(plan);
        auto child = CreateExecutor(context, plan->GetChildren()[0]);
        return std::make_unique<LimitExecutor>(context, std::move(limit_operator), std::move(child));
      }
      case OperatorType::ORDERBY: {
        auto orderby_operator = std::dynamic_pointer_cast<const OrderByOperator>(plan);
        auto child = CreateExecutor(context, plan->GetChildren()[0]);
        return std::make_unique<OrderByExecutor>(context, std::move(orderby_operator), std::move(child));
      }
      case OperatorType::LOCK_ROWS: {
        auto lock_rows_operator = std::dynamic_pointer_cast<const LockRowsOperator>(plan);
        auto child = CreateExecutor(context, plan->GetChildren()[0]);
        return std::make_unique<LockRowsExecutor>(context, std::move(lock_rows_operator), std::move(child));
      }
      case OperatorType::AGGREGATE: {
        auto aggregate_operator = std::dynamic_pointer_cast<const AggregateOperator>(plan);
        auto child = CreateExecutor(context, plan->GetChildren()[0]);
        return std::make_unique<AggregateExecutor>(context, std::move(aggregate_operator), std::move(child));
      }
      default:
        throw DbException("Unknown operator type");
    }
  }
};

}  // namespace huadb
