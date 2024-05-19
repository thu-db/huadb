#include "optimizer/optimizer.h"

namespace huadb {

Optimizer::Optimizer(Catalog &catalog, JoinOrderAlgorithm join_order_algorithm, bool enable_projection_pushdown)
    : catalog_(catalog),
      join_order_algorithm_(join_order_algorithm),
      enable_projection_pushdown_(enable_projection_pushdown) {}

std::shared_ptr<Operator> Optimizer::Optimize(std::shared_ptr<Operator> plan) {
  plan = SplitPredicates(plan);
  plan = PushDown(plan);
  plan = ReorderJoin(plan);
  return plan;
}

std::shared_ptr<Operator> Optimizer::SplitPredicates(std::shared_ptr<Operator> plan) {
  // 分解复合的选择谓词
  // 遍历查询计划树，判断每个节点是否为 Filter 节点
  // 判断 Filter 节点的谓词是否为逻辑表达式 (GetExprType() 是否为 OperatorExpressionType::LOGIC)
  // 以及逻辑表达式是否为 AND 类型 (GetLogicType() 是否为 LogicType::AND)
  // 如果是，将谓词的左右子表达式作为新的 Filter 节点添加到查询计划树中
  // LAB 5 BEGIN
  return plan;
}

std::shared_ptr<Operator> Optimizer::PushDown(std::shared_ptr<Operator> plan) {
  switch (plan->GetType()) {
    case OperatorType::FILTER:
      return PushDownFilter(std::move(plan));
    case OperatorType::PROJECTION:
      return PushDownProjection(std::move(plan));
    case OperatorType::NESTEDLOOP:
      return PushDownJoin(std::move(plan));
    case OperatorType::SEQSCAN:
      return PushDownSeqScan(std::move(plan));
    default: {
      for (auto &child : plan->children_) {
        child = SplitPredicates(child);
      }
      return plan;
    }
  }
}

std::shared_ptr<Operator> Optimizer::PushDownFilter(std::shared_ptr<Operator> plan) {
  // 将 plan 转为 FilterOperator 类型
  // 判断谓词（FilterOperator 的 predicate_ 字段）是否为 Comparison 类型，如果是，判断是否为 ColumnValue 和 ColumnValue
  // 的比较 若是，则该谓词为连接谓词；若不是，则该谓词为普通谓词
  // 可以将连接谓词和普通谓词存储到成员变量中，在遍历下层节点（SeqScan 和 NestedLoopJoin）时使用
  // 遍历结束后，根据谓词是否被成功下推（可以在 PushDownSeqScan 中记录），来决定 Filter
  // 节点是否还需在查询计划树种的原位置保留 若成功下推，则无需保留，通过 return plan->children_[0] 来删除节点
  // 否则，直接 return plan，保留节点
  // LAB 5 BEGIN
  plan->children_[0] = PushDown(plan->children_[0]);
  return plan;
}

std::shared_ptr<Operator> Optimizer::PushDownProjection(std::shared_ptr<Operator> plan) {
  // LAB 5 ADVANCED BEGIN
  plan->children_[0] = PushDown(plan->children_[0]);
  return plan;
}

std::shared_ptr<Operator> Optimizer::PushDownJoin(std::shared_ptr<Operator> plan) {
  // ColumnValue 的 name_ 字段为 "table_name.column_name" 的形式
  // 判断当前查询计划树的连接谓词是否使用当前 NestedLoopJoin 节点涉及到的列
  // 如果是，将连接谓词添加到当前的 NestedLoopJoin 节点的 join_condition_ 中
  // LAB 5 BEGIN
  for (auto &child : plan->children_) {
    child = PushDown(child);
  }
  return plan;
}

std::shared_ptr<Operator> Optimizer::PushDownSeqScan(std::shared_ptr<Operator> plan) {
  // ColumnValue 的 name_ 字段为 "table_name.column_name" 的形式
  // 根据 table_name 与 SeqScanOperator 的 GetTableNameOrAlias 判断谓词是否匹配当前 SeqScan 节点的表
  // 如果匹配，在此扫描节点上方添加 Filter 节点，并将其作为返回值
  // LAB 5 BEGIN
  return plan;
}

std::shared_ptr<Operator> Optimizer::ReorderJoin(std::shared_ptr<Operator> plan) {
  // 通过 catalog_.GetCardinality 和 catalog_.GetDistinct 从系统表中读取表和列的元信息
  // 可根据 join_order_algorithm_ 变量的值选择不同的连接顺序选择算法，默认为 None，表示不进行连接顺序优化
  // LAB 5 BEGIN
  return plan;
}

}  // namespace huadb
