#include "executors/orderby_executor.h"

namespace huadb {

OrderByExecutor::OrderByExecutor(ExecutorContext &context, std::shared_ptr<const OrderByOperator> plan,
                                 std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void OrderByExecutor::Init() { children_[0]->Init(); }

std::shared_ptr<Record> OrderByExecutor::Next() {
  // 可以使用 STL 的 sort 函数
  // 通过 OperatorExpression 的 Evaluate 函数获取 Value 的值
  // 通过 Value 的 Less, Equal, Greater 函数比较 Value 的值
  // LAB 4 BEGIN
  return children_[0]->Next();
}

}  // namespace huadb
