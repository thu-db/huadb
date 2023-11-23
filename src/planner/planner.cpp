//===----------------------------------------------------------------------===//
// Copyright (c) 2019 CMU Database Group

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//===----------------------------------------------------------------------===//

#include "planner/planner.h"

#include <optional>
#include <string>

#include "binder/expressions/expressions.h"
#include "binder/statement.h"
#include "binder/statements/delete_statement.h"
#include "binder/statements/insert_statement.h"
#include "binder/statements/select_statement.h"
#include "binder/statements/update_statement.h"
#include "binder/table_refs/table_refs.h"
#include "common/exceptions.h"
#include "common/string_util.h"
#include "operators/expressions/expressions.h"
#include "operators/operators.h"

namespace huadb {

Planner::Planner(ForceJoin force_join) : force_join_(force_join) {}

std::shared_ptr<Operator> Planner::PlanQuery(const Statement &stmt) {
  switch (stmt.type_) {
    case StatementType::INSERT_STATEMENT:
      return PlanInsert(dynamic_cast<const InsertStatement &>(stmt));
    case StatementType::DELETE_STATEMENT:
      return PlanDelete(dynamic_cast<const DeleteStatement &>(stmt));
    case StatementType::UPDATE_STATEMENT:
      return PlanUpdate(dynamic_cast<const UpdateStatement &>(stmt));
    case StatementType::SELECT_STATEMENT:
      return PlanSelect(dynamic_cast<const SelectStatement &>(stmt));
    default:
      throw DbException("Statement Type not implemented in planner");
  }
}

std::shared_ptr<Operator> Planner::PlanInsert(const InsertStatement &stmt) {
  auto select = PlanSelect(*stmt.select_stmt_);

  auto child_columns = select->OutputColumns();
  ColumnList table_columns;
  if (stmt.columns_.empty()) {
    table_columns = stmt.table_->column_list_;
  } else {
    auto all_columns = stmt.table_->column_list_;
    for (const auto &column : stmt.columns_) {
      auto col_idx = all_columns.GetColumnIndex(StringUtil::Split(column->ToString(), '.')[1]);
      table_columns.AddColumn(all_columns.GetColumn(col_idx));
    }
  }
  if (child_columns.Length() != table_columns.Length()) {
    throw DbException("Insert column number does not match table column number");
  }
  auto length = child_columns.Length();
  for (size_t i = 0; i < length; i++) {
    auto child_type = child_columns.GetColumn(i).type_;
    auto table_type = table_columns.GetColumn(i).type_;
    if (child_type != table_type && child_type != Type::NULL_TYPE) {
      if (!(child_type == Type::VARCHAR && table_type == Type::CHAR)) {
        throw DbException("Insert column type does not match table column type");
      }
    }
    if (TypeUtil::IsString(child_type) && TypeUtil::IsString(table_type)) {
      if (child_columns.GetColumn(i).GetMaxSize() > table_columns.GetColumn(i).GetMaxSize()) {
        throw DbException("Insert value too long");
      }
    }
  }

  auto column_list = std::make_shared<ColumnList>();
  column_list->AddColumn(ColumnDefinition("insert", Type::INT));

  return std::make_shared<InsertOperator>(std::move(column_list), std::move(select), std::move(table_columns),
                                          stmt.table_->oid_);
}

std::shared_ptr<Operator> Planner::PlanDelete(const DeleteStatement &stmt) {
  auto table = PlanTableRef(*stmt.table_);
  auto filter_expr = PlanExpression(*stmt.filter_, {table});
  auto filter_column_list = std::make_shared<ColumnList>(table->OutputColumns());
  auto filter_node = std::make_shared<FilterOperator>(filter_column_list, std::move(table), std::move(filter_expr));

  auto column_list = std::make_shared<ColumnList>();
  column_list->AddColumn(ColumnDefinition("delete", Type::INT));
  return std::make_shared<DeleteOperator>(std::move(column_list), std::move(filter_node), stmt.table_->oid_);
}

std::shared_ptr<Operator> Planner::PlanUpdate(const UpdateStatement &stmt) {
  auto table = PlanTableRef(*stmt.table_);
  auto filter_expr = PlanExpression(*stmt.filter_, {table});
  auto filter_column_list = std::make_shared<ColumnList>(table->OutputColumns());
  auto filter_node = std::make_shared<FilterOperator>(filter_column_list, std::move(table), std::move(filter_expr));

  std::vector<std::shared_ptr<OperatorExpression>> update_exprs(filter_node->OutputColumns().Length());
  for (const auto &update_expr : stmt.update_exprs_) {
    auto column = PlanColumnRef(*update_expr.first, {filter_node});
    auto expr = PlanExpression(*update_expr.second, {filter_node});
    update_exprs[column->GetColumnIndex()] = std::move(expr);
  }
  for (auto i = 0; i < update_exprs.size(); i++) {
    if (update_exprs[i] == nullptr) {
      update_exprs[i] = std::make_shared<ColumnValue>(i, filter_node->OutputColumns().GetColumn(i).type_,
                                                      filter_node->OutputColumns().GetColumn(i).name_,
                                                      filter_node->OutputColumns().GetColumn(i).max_size_, true);
    }
  }

  auto column_list = std::make_shared<ColumnList>();
  column_list->AddColumn(ColumnDefinition("update", Type::INT));
  return std::make_shared<UpdateOperator>(std::move(column_list), std::move(filter_node), stmt.table_->oid_,
                                          std::move(update_exprs));
}

std::shared_ptr<Operator> Planner::PlanSelect(const SelectStatement &stmt) {
  std::shared_ptr<Operator> plan;
  switch (stmt.table_->type_) {
    case TableRefType::EMPTY:
      plan = std::make_shared<ValuesOperator>(std::make_shared<ColumnList>(std::vector<ColumnDefinition>()),
                                              std::vector<std::vector<std::shared_ptr<OperatorExpression>>>{
                                                  std::vector<std::shared_ptr<OperatorExpression>>{}});
      break;
    default:
      plan = PlanTableRef(*stmt.table_, stmt.lock_type_ != SelectLockType::NOLOCK);
  }

  if (stmt.where_ != nullptr) {
    auto column_list = std::make_shared<ColumnList>(plan->OutputColumns());
    auto expr = PlanExpression(*stmt.where_, {plan});
    plan = std::make_shared<FilterOperator>(std::move(column_list), std::move(plan), std::move(expr));
  }

  bool has_agg = false;
  for (const auto &item : stmt.select_list_) {
    if (item->type_ == ExpressionType::ALIAS) {
      const auto alias_expr = dynamic_cast<const AliasExpression &>(*item);
      if (alias_expr.expr_->type_ != ExpressionType::AGGREGATE) {
        PlanExpression(*item, {plan});  // Add alias
      }
    }
    if (item->HasAggregation()) {
      has_agg = true;
    }
  }

  std::vector<std::shared_ptr<OperatorExpression>> exprs;
  std::vector<std::string> column_names;
  if (has_agg || !stmt.group_by_.empty()) {
    plan = PlanAggregate(stmt, std::move(plan));
  }
  for (const auto &item : stmt.select_list_) {
    auto expr = PlanExpression(*item, {plan});
    column_names.push_back(expr->name_);
    exprs.emplace_back(std::move(expr));
  }

  if (!stmt.order_by_.empty()) {
    std::vector<std::pair<OrderByType, std::shared_ptr<OperatorExpression>>> order_bys;
    for (const auto &order_by : stmt.order_by_) {
      auto expr = PlanExpression(*order_by->expr_, {plan});
      order_bys.emplace_back(std::make_pair(order_by->type_, std::move(expr)));
    }
    auto column_list = std::make_shared<ColumnList>(plan->OutputColumns());
    plan = std::make_shared<OrderByOperator>(column_list, std::move(plan), std::move(order_bys));
  }

  if ((stmt.limit_count_ != nullptr) || (stmt.limit_offset_ != nullptr)) {
    std::optional<int32_t> limit_count;
    std::optional<int32_t> limit_offset;
    if (stmt.limit_count_ != nullptr) {
      auto expr = PlanExpression(*stmt.limit_count_, {plan});
      limit_count = expr->Evaluate(std::make_shared<Record>()).GetValue<int32_t>();
      if (limit_count < 0) {
        throw DbException("LIMIT must not be negative");
      }
    }
    if (stmt.limit_offset_ != nullptr) {
      auto expr = PlanExpression(*stmt.limit_offset_, {plan});
      limit_offset = expr->Evaluate(std::make_shared<Record>()).GetValue<int32_t>();
      if (limit_offset < 0) {
        throw DbException("OFFSET must not be negative");
      }
    }
    auto column_list = std::make_shared<ColumnList>(plan->OutputColumns());
    plan = std::make_shared<LimitOperator>(std::move(column_list), std::move(plan), limit_count, limit_offset);
  }

  if (stmt.lock_type_ != SelectLockType::NOLOCK) {
    if (stmt.table_->type_ != TableRefType::BASE_TABLE) {
      throw DbException("Locking only supports for base table");
    }
    auto &table = dynamic_cast<BaseTableRef &>(*stmt.table_);
    auto column_list = std::make_shared<ColumnList>(plan->OutputColumns());
    plan = std::make_shared<LockRowsOperator>(std::move(column_list), std::move(plan), table.oid_, stmt.lock_type_);
  }
  auto column_list = RenameColumnList(InferColumnList(exprs), column_names);
  plan = std::make_shared<ProjectionOperator>(column_list, std::move(plan), std::move(exprs));
  if (stmt.distinct_) {
    std::vector<std::shared_ptr<OperatorExpression>> distinct_exprs;
    auto col_idx = 0;
    for (const auto &col : plan->OutputColumns().GetColumns()) {
      distinct_exprs.emplace_back(
          std::make_shared<ColumnValue>(col_idx++, col.type_, col.name_, col.GetMaxSize(), true));
    }
    auto column_list = std::make_shared<ColumnList>(plan->OutputColumns());
    plan = std::make_shared<AggregateOperator>(column_list, std::move(plan), std::move(distinct_exprs),
                                               std::vector<std::shared_ptr<OperatorExpression>>{}, std::vector<bool>{},
                                               std::vector<AggregateType>{});
  }
  return plan;
}

std::shared_ptr<OperatorExpression> Planner::PlanExpression(const Expression &expr,
                                                            const std::vector<std::shared_ptr<Operator>> &children) {
  switch (expr.type_) {
    case ExpressionType::CONST: {
      const auto &const_expr = dynamic_cast<const ConstExpression &>(expr);
      return PlanConst(const_expr, children);
    }
    case ExpressionType::UNARY_OP: {
      const auto &unary_op_expr = dynamic_cast<const UnaryOpExpression &>(expr);
      return PlanUnaryOp(unary_op_expr, children);
    }
    case ExpressionType::BINARY_OP: {
      const auto &binary_op_expr = dynamic_cast<const BinaryOpExpression &>(expr);
      return PlanBinaryOp(binary_op_expr, children);
    }
    case ExpressionType::COLUMN_REF: {
      const auto &column_ref_expr = dynamic_cast<const ColumnRefExpression &>(expr);
      return PlanColumnRef(column_ref_expr, children);
    }
    case ExpressionType::ALIAS: {
      const auto &alias_expr = dynamic_cast<const AliasExpression &>(expr);
      auto bound_expr = PlanExpression(*alias_expr.expr_, children);
      bool add_to_alias = true;
      for (auto [it, range_end] = alias2colname_.equal_range(alias_expr.name_); it != range_end; it++) {
        if (it->second == bound_expr->ToString()) {
          add_to_alias = false;
          break;
        }
      }
      if (add_to_alias) {
        aliases_.emplace(alias_expr.name_, bound_expr);
        alias2colname_[alias_expr.name_] = bound_expr->name_;
      }
      bound_expr->SetName(alias_expr.name_);
      return bound_expr;
    }
    case ExpressionType::AGGREGATE: {
      if (next_aggregate_ >= aggregate_exprs_.size()) {
        throw DbException("Too many aggregate calls in PlanExpression");
      }
      return std::move(aggregate_exprs_[next_aggregate_++]);
    }
    case ExpressionType::LIST: {
      const auto &list_expr = dynamic_cast<const ListExpression &>(expr);
      std::vector<std::shared_ptr<OperatorExpression>> exprs;
      for (const auto &item : list_expr.exprs_) {
        exprs.push_back(PlanExpression(*item, children));
      }
      return std::make_shared<List>(std::move(exprs));
    }
    case ExpressionType::TYPE_CAST: {
      const auto &cast_expr = dynamic_cast<const TypeCastExpression &>(expr);
      auto arg = PlanExpression(*cast_expr.arg_, children);
      return std::make_shared<TypeCast>(cast_expr.cast_type_, std::move(arg));
    }
    case ExpressionType::NULL_TEST: {
      const auto &null_expr = dynamic_cast<const NullTestExpression &>(expr);
      auto arg = PlanExpression(*null_expr.arg_, children);
      return std::make_shared<NullTest>(null_expr.is_null_, std::move(arg));
    }
    case ExpressionType::FUNC_CALL: {
      const auto &func_call_expr = dynamic_cast<const FuncCallExpression &>(expr);
      std::vector<std::shared_ptr<OperatorExpression>> args;
      for (const auto &arg : func_call_expr.args_) {
        args.push_back(PlanExpression(*arg, children));
      }
      return std::make_shared<FuncCall>(func_call_expr.function_name_, std::move(args));
    }
    default:
      throw DbException("Unsupported expression type in PlanExpression");
  }
}

std::shared_ptr<OperatorExpression> Planner::PlanConst(const ConstExpression &expr,
                                                       const std::vector<std::shared_ptr<Operator>> &children) {
  return std::make_shared<Const>(expr.value_);
}

std::shared_ptr<OperatorExpression> Planner::PlanUnaryOp(const UnaryOpExpression &expr,
                                                         const std::vector<std::shared_ptr<Operator>> &children) {
  auto arg = PlanExpression(*expr.arg_, children);
  if (expr.op_name_ == "not") {
    return std::make_shared<Logic>(LogicType::NOT, std::move(arg));
  } else {
    throw DbException("Unknown unary operator " + expr.op_name_);
  }
}

std::shared_ptr<OperatorExpression> Planner::PlanBinaryOp(const BinaryOpExpression &expr,
                                                          const std::vector<std::shared_ptr<Operator>> &children) {
  auto left = PlanExpression(*expr.left_, children);
  auto right = PlanExpression(*expr.right_, children);
  return BinaryFactory(expr.op_name_, std::move(left), std::move(right));
}

std::shared_ptr<ColumnValue> Planner::PlanColumnRef(const ColumnRefExpression &expr,
                                                    const std::vector<std::shared_ptr<Operator>> &children) {
  if (children.empty()) {
    throw DbException("No children in PlanColumnRef");
  }
  auto col_name = expr.ToString();
  if (children.size() == 1) {
    auto column_list = children[0]->OutputColumns();
    if (column_list.TryGetColumnIndex(col_name)) {
      auto col_idx = column_list.GetColumnIndex(col_name);
      auto col_type = column_list.GetColumn(col_idx).type_;
      auto col_size = column_list.GetColumn(col_idx).GetMaxSize();
      return std::make_shared<ColumnValue>(col_idx, col_type, col_name, col_size, true);
    } else if (aliases_.find(col_name) != aliases_.end()) {
      if (aliases_.count(col_name) > 1) {
        throw DbException("Column name " + col_name + " is ambiguous in PlanColumnRef");
      }
      if (aliases_.find(col_name)->second->GetExprType() == OperatorExpressionType::COLUMN_VALUE) {
        return std::dynamic_pointer_cast<ColumnValue>(aliases_.find(col_name)->second);
      }
    }
    throw DbException("Column " + col_name + " not found in planner");
  } else if (children.size() == 2) {
    auto column_list_left = children[0]->OutputColumns();
    auto column_list_right = children[1]->OutputColumns();
    auto col_idx_left = column_list_left.TryGetColumnIndex(col_name);
    auto col_idx_right = column_list_right.TryGetColumnIndex(col_name);
    if (col_idx_left && col_idx_right) {
      throw DbException("Ambiguous column name " + col_name + " in PlanColumnRef");
    }
    if (col_idx_left) {
      auto col_type = column_list_left.GetColumn(*col_idx_left).type_;
      auto col_size = column_list_left.GetColumn(*col_idx_left).max_size_;
      return std::make_shared<ColumnValue>(*col_idx_left, col_type, col_name, col_size, true);
    }
    if (col_idx_right) {
      auto col_type = column_list_right.GetColumn(*col_idx_right).type_;
      auto col_size = column_list_left.GetColumn(*col_idx_right).max_size_;
      return std::make_shared<ColumnValue>(*col_idx_right, col_type, col_name, col_size, false);
    }
    throw DbException(col_name + " not found in PlanColumnRef");
  } else {
    throw DbException("Too many children in PlanColumnRef");
  }
}

std::tuple<AggregateType, bool, std::shared_ptr<OperatorExpression>> Planner::GetAggregateType(
    const AggregateExpression &expr, const std::vector<std::shared_ptr<Operator>> &children) {
  if (expr.args_.size() == 0) {
    return {AggregateType::COUNT_STAR, expr.is_distinct_, std::make_shared<OperatorExpression>()};
  }
  if (expr.args_.size() > 1) {
    throw DbException("Too many arguments in aggregate function");
  }
  auto arg_expr = PlanExpression(*expr.args_[0], children);
  if (expr.function_name_ == "avg") {
    return {AggregateType::AVG, expr.is_distinct_, std::move(arg_expr)};
  } else if (expr.function_name_ == "count") {
    return {AggregateType::COUNT, expr.is_distinct_, std::move(arg_expr)};
  } else if (expr.function_name_ == "sum") {
    return {AggregateType::SUM, expr.is_distinct_, std::move(arg_expr)};
  } else if (expr.function_name_ == "max") {
    return {AggregateType::MAX, expr.is_distinct_, std::move(arg_expr)};
  } else if (expr.function_name_ == "min") {
    return {AggregateType::MIN, expr.is_distinct_, std::move(arg_expr)};
  } else {
    throw DbException("Unknown function name " + expr.function_name_);
  }
}

std::shared_ptr<Operator> Planner::PlanAggregate(const SelectStatement &stmt, std::shared_ptr<Operator> child) {
  std::unordered_set<std::string> group_by_names;
  std::vector<std::string> output_column_names;
  std::vector<std::shared_ptr<OperatorExpression>> group_bys;
  std::vector<std::shared_ptr<OperatorExpression>> aggregates;
  std::vector<bool> is_distincts;
  std::vector<AggregateType> aggregate_types;
  for (const auto &item : stmt.group_by_) {
    auto expr = PlanExpression(*item, {child});
    group_by_names.insert(expr->name_);
    if (aliases_.find(expr->name_) != aliases_.end()) {
      output_column_names.push_back(alias2colname_[expr->name_]);
    } else {
      output_column_names.push_back(std::move(expr->name_));
    }
    group_bys.push_back(std::move(expr));
  }
  auto agg_begin = group_bys.size();
  size_t agg_index = 0;
  if (stmt.having_ != nullptr) {
    AddAggregateExpression(*stmt.having_);
  }
  for (const auto &item : stmt.select_list_) {
    AddAggregateExpression(*item);
  }
  for (const auto &item : stmt.order_by_) {
    AddAggregateExpression(*item->expr_);
  }
  for (const auto &item : aggregates_) {
    assert(item->type_ == ExpressionType::AGGREGATE);
    const auto &aggregate_expr = dynamic_cast<const AggregateExpression &>(*item);
    auto agg_tuple = GetAggregateType(aggregate_expr, {child});
    aggregate_exprs_.push_back(std::make_shared<ColumnValue>(
        agg_begin + agg_index, Type::INT, aggregate_expr.function_name_, TypeUtil::TypeSize(Type::INT)));
    agg_index++;
    aggregate_types.push_back(std::get<0>(agg_tuple));
    is_distincts.push_back(std::get<1>(agg_tuple));
    if (std::get<0>(agg_tuple) == AggregateType::COUNT_STAR) {
      aggregates.push_back(std::make_shared<Const>(Value(1)));
    } else {
      aggregates.push_back(std::get<2>(agg_tuple));
    }
    output_column_names.emplace_back(aggregate_expr.function_name_);
  }

  for (const auto &item : stmt.select_list_) {
    CheckAggregate(*item, group_by_names);
  }
  std::shared_ptr<Operator> plan = std::make_shared<AggregateOperator>(
      RenameColumnList(InferAggregateColumnList(group_bys, aggregates), output_column_names), std::move(child),
      std::move(group_bys), std::move(aggregates), std::move(is_distincts), std::move(aggregate_types));
  if (stmt.having_ != nullptr) {
    auto expr = PlanExpression(*stmt.having_, {plan});
    plan = std::make_shared<FilterOperator>(std::make_shared<ColumnList>(plan->OutputColumns()), std::move(plan),
                                            std::move(expr));
  }
  return plan;
}

std::shared_ptr<Operator> Planner::PlanTableRef(const TableRef &ref, bool has_lock) {
  switch (ref.type_) {
    case TableRefType::BASE_TABLE: {
      const auto &base_table_ref = dynamic_cast<const BaseTableRef &>(ref);
      return PlanBaseTable(base_table_ref, has_lock);
    }
    case TableRefType::EXPRESSION_LIST: {
      const auto &expression_list_ref = dynamic_cast<const ExpressionListRef &>(ref);
      return PlanExpressionList(expression_list_ref);
    }
    case TableRefType::CROSS_JOIN: {
      const auto &cross_join_ref = dynamic_cast<const CrossJoinRef &>(ref);
      return PlanCrossJoin(cross_join_ref);
    }
    case TableRefType::JOIN: {
      const auto &join_ref = dynamic_cast<const JoinRef &>(ref);
      return PlanJoin(join_ref);
    }
    default:
      throw DbException("TableRef type not supported in PlanTableRef");
  }
}

std::shared_ptr<Operator> Planner::PlanBaseTable(const BaseTableRef &ref, bool has_lock) {
  return std::make_shared<SeqScanOperator>(GetColumnList(ref), ref.oid_, ref.table_, ref.alias_, has_lock);
}

std::shared_ptr<Operator> Planner::PlanExpressionList(const ExpressionListRef &ref) {
  std::vector<std::vector<std::shared_ptr<OperatorExpression>>> exprs_list;
  for (const auto &values : ref.values_list_) {
    std::vector<std::shared_ptr<OperatorExpression>> exprs;
    for (const auto &value : values) {
      auto expr = PlanExpression(*value, {});
      exprs.push_back(std::move(expr));
    }
    exprs_list.push_back(std::move(exprs));
  }

  auto column_list = std::make_shared<ColumnList>();
  size_t idx = 1;
  for (const auto &expr : exprs_list[0]) {
    if (TypeUtil::IsString(expr->GetValueType())) {
      assert(expr->GetExprType() == OperatorExpressionType::CONST);
      column_list->AddColumn(ColumnDefinition("#" + std::to_string(idx), expr->GetValueType(), expr->GetSize()));
    } else {
      column_list->AddColumn(ColumnDefinition("#" + std::to_string(idx), expr->GetValueType()));
    }
    idx++;
  }

  return std::make_shared<ValuesOperator>(std::move(column_list), std::move(exprs_list));
}

std::shared_ptr<Operator> Planner::PlanCrossJoin(const CrossJoinRef &ref) {
  auto left = PlanTableRef(*ref.left_);
  auto right = PlanTableRef(*ref.right_);
  return std::make_shared<NestedLoopJoinOperator>(GetJoinColumnList(*left, *right), std::move(left), std::move(right),
                                                  std::make_shared<Const>(Value(true)));
}

std::shared_ptr<Operator> Planner::PlanJoin(const JoinRef &ref) {
  auto left = PlanTableRef(*ref.left_);
  auto right = PlanTableRef(*ref.right_);
  const auto join_condition = PlanExpression(*ref.condition_, {left, right});

  if (force_join_ == ForceJoin::MERGE) {
    if (join_condition->GetExprType() == OperatorExpressionType::COMPARISON) {
      const auto expr = std::dynamic_pointer_cast<Comparison>(join_condition);
      if (expr->GetComparisonType() == ComparisonType::EQUAL &&
          expr->children_[0]->GetExprType() == OperatorExpressionType::COLUMN_VALUE &&
          expr->children_[1]->GetExprType() == OperatorExpressionType::COLUMN_VALUE) {
        auto left_key = std::dynamic_pointer_cast<ColumnValue>(expr->children_[0]);
        auto right_key = std::dynamic_pointer_cast<ColumnValue>(expr->children_[1]);
        auto column_list = GetJoinColumnList(*left, *right);
        auto left_column_list = std::make_shared<ColumnList>(left->OutputColumns());
        auto right_column_list = std::make_shared<ColumnList>(right->OutputColumns());
        auto left_order =
            std::make_shared<OrderByOperator>(std::move(left_column_list), std::move(left),
                                              std::vector<std::pair<OrderByType, std::shared_ptr<OperatorExpression>>>{
                                                  std::make_pair(OrderByType::ASC, left_key)});
        auto right_order =
            std::make_shared<OrderByOperator>(std::move(right_column_list), std::move(right),
                                              std::vector<std::pair<OrderByType, std::shared_ptr<OperatorExpression>>>{
                                                  std::make_pair(OrderByType::ASC, right_key)});
        return std::make_shared<MergeJoinOperator>(std::move(column_list), std::move(left_order),
                                                   std::move(right_order), std::move(left_key), std::move(right_key),
                                                   ref.join_type_);
      }
    }
  } else if (force_join_ == ForceJoin::HASH) {
    if (join_condition->GetExprType() == OperatorExpressionType::COMPARISON) {
      const auto expr = std::dynamic_pointer_cast<Comparison>(join_condition);
      if (expr->GetComparisonType() == ComparisonType::EQUAL &&
          expr->children_[0]->GetExprType() == OperatorExpressionType::COLUMN_VALUE &&
          expr->children_[1]->GetExprType() == OperatorExpressionType::COLUMN_VALUE) {
        auto left_key = std::dynamic_pointer_cast<ColumnValue>(expr->children_[0]);
        auto right_key = std::dynamic_pointer_cast<ColumnValue>(expr->children_[1]);
        auto column_list = GetJoinColumnList(*left, *right);
        return std::make_shared<HashJoinOperator>(std::move(column_list), std::move(left), std::move(right),
                                                  std::move(left_key), std::move(right_key), ref.join_type_);
      }
    }
  }
  auto column_list = GetJoinColumnList(*left, *right);
  return std::make_shared<NestedLoopJoinOperator>(std::move(column_list), std::move(left), std::move(right),
                                                  std::move(join_condition), ref.join_type_);
}

std::shared_ptr<OperatorExpression> Planner::BinaryFactory(const std::string &op_name,
                                                           std::shared_ptr<OperatorExpression> left,
                                                           std::shared_ptr<OperatorExpression> right) {
  if (op_name == "+") {
    return std::make_shared<Arithmetic>(ArithmeticType::ADD, std::move(left), std::move(right));
  } else if (op_name == "-") {
    return std::make_shared<Arithmetic>(ArithmeticType::SUB, std::move(left), std::move(right));
  } else if (op_name == "*") {
    return std::make_shared<Arithmetic>(ArithmeticType::MUL, std::move(left), std::move(right));
  } else if (op_name == "/") {
    return std::make_shared<Arithmetic>(ArithmeticType::DIV, std::move(left), std::move(right));
  } else if (op_name == "=" && right->GetExprType() == OperatorExpressionType::LIST) {
    return std::make_shared<Comparison>(ComparisonType::IN, std::move(left), std::move(right));
  } else if (op_name == "=") {
    return std::make_shared<Comparison>(ComparisonType::EQUAL, std::move(left), std::move(right));
  } else if (((op_name == "!=") || (op_name == "<>")) && right->GetExprType() == OperatorExpressionType::LIST) {
    return std::make_shared<Comparison>(ComparisonType::NOT_IN, std::move(left), std::move(right));
  } else if ((op_name == "!=") || (op_name == "<>")) {
    return std::make_shared<Comparison>(ComparisonType::NOT_EQUAL, std::move(left), std::move(right));
  } else if (op_name == "<") {
    return std::make_shared<Comparison>(ComparisonType::LESS, std::move(left), std::move(right));
  } else if (op_name == "<=") {
    return std::make_shared<Comparison>(ComparisonType::LESS_EQUAL, std::move(left), std::move(right));
  } else if (op_name == ">") {
    return std::make_shared<Comparison>(ComparisonType::GREATER, std::move(left), std::move(right));
  } else if (op_name == ">=") {
    return std::make_shared<Comparison>(ComparisonType::GREATER_EQUAL, std::move(left), std::move(right));
  } else if (op_name == "BETWEEN") {
    return std::make_shared<Comparison>(ComparisonType::BETWEEN, std::move(left), std::move(right));
  } else if (op_name == "NOT BETWEEN") {
    return std::make_shared<Comparison>(ComparisonType::NOT_BETWEEN, std::move(left), std::move(right));
  } else if (op_name == "~~") {
    return std::make_shared<Comparison>(ComparisonType::LIKE, std::move(left), std::move(right));
  } else if (op_name == "!~~") {
    return std::make_shared<Comparison>(ComparisonType::NOT_LIKE, std::move(left), std::move(right));
  } else if (op_name == "and") {
    return std::make_shared<Logic>(LogicType::AND, std::move(left), std::move(right));
  } else if (op_name == "or") {
    return std::make_shared<Logic>(LogicType::OR, std::move(left), std::move(right));
  }
  throw DbException("Unsupported binary operator in BinaryFactory: " + op_name);
}

std::shared_ptr<ColumnList> Planner::GetColumnList(const BaseTableRef &table) const {
  auto column_list = std::make_shared<ColumnList>();
  for (const auto &column : table.column_list_.GetColumns()) {
    column_list->AddColumn(ColumnDefinition(fmt::format("{}.{}", table.GetTableName(), column.name_), column.GetType(),
                                            column.GetMaxSize()));
  }
  return column_list;
}

std::shared_ptr<ColumnList> Planner::InferColumnList(const std::vector<std::shared_ptr<OperatorExpression>> &exprs) {
  auto column_list = std::make_shared<ColumnList>();
  size_t idx = 1;
  for (const auto &expr : exprs) {
    auto type = expr->GetValueType();
    if (TypeUtil::IsString(type)) {
      assert(expr->GetExprType() == OperatorExpressionType::COLUMN_VALUE ||
             expr->GetExprType() == OperatorExpressionType::CONST ||
             expr->GetExprType() == OperatorExpressionType::FUNC_CALL);
      column_list->AddColumn(ColumnDefinition("#" + std::to_string(idx), type, expr->GetSize()));
    } else {
      column_list->AddColumn(ColumnDefinition("#" + std::to_string(idx), type));
    }
    idx++;
  }
  return column_list;
}

std::shared_ptr<ColumnList> Planner::InferAggregateColumnList(
    const std::vector<std::shared_ptr<OperatorExpression>> &group_bys,
    const std::vector<std::shared_ptr<OperatorExpression>> &aggregates) {
  auto column_list = std::make_shared<ColumnList>();
  for (const auto &group_by : group_bys) {
    if (TypeUtil::IsString(group_by->GetValueType())) {
      assert(group_by->GetExprType() == OperatorExpressionType::COLUMN_VALUE ||
             group_by->GetExprType() == OperatorExpressionType::CONST);
      column_list->AddColumn(ColumnDefinition(group_by->name_, group_by->GetValueType(), group_by->GetSize()));
    } else {
      column_list->AddColumn(ColumnDefinition(group_by->name_, group_by->GetValueType()));
    }
  }
  for (const auto &aggregate : aggregates) {
    if (TypeUtil::IsString(aggregate->GetValueType())) {
      assert(aggregate->GetExprType() == OperatorExpressionType::COLUMN_VALUE ||
             aggregate->GetExprType() == OperatorExpressionType::CONST);
      column_list->AddColumn(ColumnDefinition("no_name", aggregate->GetValueType(), aggregate->GetSize()));
    } else {
      column_list->AddColumn(ColumnDefinition("no_name", aggregate->GetValueType()));
    }
  }
  return std::move(column_list);
}

std::shared_ptr<ColumnList> Planner::GetJoinColumnList(const Operator &left, const Operator &right) {
  auto column_list = std::make_shared<ColumnList>();
  for (const auto &column : left.column_list_->GetColumns()) {
    column_list->AddColumn(column);
  }
  for (const auto &column : right.column_list_->GetColumns()) {
    column_list->AddColumn(column);
  }
  return column_list;
}

std::shared_ptr<ColumnList> Planner::RenameColumnList(std::shared_ptr<const ColumnList> column_list,
                                                      const std::vector<std::string> &col_names) {
  auto result = std::make_shared<ColumnList>();
  size_t idx = 0;
  for (const auto &column : column_list->GetColumns()) {
    result->AddColumn(ColumnDefinition(col_names[idx++], column.GetType(), column.GetMaxSize()));
  }
  return result;
}

void Planner::AddAggregateExpression(Expression &expr) {
  switch (expr.type_) {
    case ExpressionType::AGGREGATE: {
      auto &agg_expr = dynamic_cast<AggregateExpression &>(expr);
      aggregates_.push_back(std::make_unique<AggregateExpression>(
          std::exchange(agg_expr, AggregateExpression(agg_expr.function_name_, agg_expr.is_distinct_,
                                                      std::vector<std::unique_ptr<Expression>>{}))));
      return;
    }
    case ExpressionType::BINARY_OP: {
      auto &binary_op_expr = dynamic_cast<BinaryOpExpression &>(expr);
      AddAggregateExpression(*binary_op_expr.left_);
      AddAggregateExpression(*binary_op_expr.right_);
      return;
    }
    case ExpressionType::ALIAS: {
      auto &alias_expr = dynamic_cast<AliasExpression &>(expr);
      AddAggregateExpression(*alias_expr.expr_);
      return;
    }
    case ExpressionType::COLUMN_REF:
    case ExpressionType::CONST:
      return;
    default:
      throw DbException("Unknown Expression type in agg");
  }
}

void Planner::CheckAggregate(const Expression &expr, const std::unordered_set<std::string> group_by_names) {
  switch (expr.type_) {
    case ExpressionType::AGGREGATE:
    case ExpressionType::CONST:
      break;
    case ExpressionType::COLUMN_REF: {
      if (group_by_names.find(expr.ToString()) == group_by_names.end()) {
        throw DbException(expr.ToString() + " must appear in the GROUP BY clause or be used in an aggregate function");
      }
      break;
    }
    case ExpressionType::BINARY_OP: {
      const auto &binary_op_expr = dynamic_cast<const BinaryOpExpression &>(expr);
      CheckAggregate(*binary_op_expr.left_, group_by_names);
      CheckAggregate(*binary_op_expr.right_, group_by_names);
      break;
    }
    case ExpressionType::ALIAS: {
      const auto &alias_expr = dynamic_cast<const AliasExpression &>(expr);
      if (alias_expr.expr_->type_ != ExpressionType::AGGREGATE &&
          group_by_names.find(alias_expr.expr_->ToString()) == group_by_names.end() &&
          group_by_names.find(alias_expr.name_) == group_by_names.end()) {
        throw DbException(alias_expr.name_ + " must appear in the GROUP BY clause or be used in an aggregate function");
      }
      break;
    }
    default:
      throw DbException("Unknown Expression type in CheckAggregate");
  }
}

}  // namespace huadb
