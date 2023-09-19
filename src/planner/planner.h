#pragma once

#include <memory>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/column_list.h"
#include "operators/expressions/expression.h"
#include "operators/operator.h"

namespace huadb {

class Statement;
class InsertStatement;
class DeleteStatement;
class UpdateStatement;
class SelectStatement;

class Expression;
class ConstExpression;
class BinaryOpExpression;
class UnaryOpExpression;
class ColumnRefExpression;
class AggregateExpression;

class TableRef;
class BaseTableRef;
class ExpressionListRef;
class CrossJoinRef;
class JoinRef;

class ColumnValue;
enum class AggregateType;

enum class ForceJoin { NONE, MERGE, HASH };

class Planner {
 public:
  Planner(ForceJoin force_join);

  std::shared_ptr<Operator> PlanQuery(const Statement &stmt);
  std::shared_ptr<Operator> PlanInsert(const InsertStatement &stmt);
  std::shared_ptr<Operator> PlanDelete(const DeleteStatement &stmt);
  std::shared_ptr<Operator> PlanUpdate(const UpdateStatement &stmt);
  std::shared_ptr<Operator> PlanSelect(const SelectStatement &stmt);

  std::shared_ptr<OperatorExpression> PlanExpression(const Expression &expr,
                                                     const std::vector<std::shared_ptr<Operator>> &children);
  std::shared_ptr<OperatorExpression> PlanConst(const ConstExpression &expr,
                                                const std::vector<std::shared_ptr<Operator>> &children);
  std::shared_ptr<OperatorExpression> PlanUnaryOp(const UnaryOpExpression &expr,
                                                  const std::vector<std::shared_ptr<Operator>> &children);
  std::shared_ptr<OperatorExpression> PlanBinaryOp(const BinaryOpExpression &expr,
                                                   const std::vector<std::shared_ptr<Operator>> &children);
  std::shared_ptr<ColumnValue> PlanColumnRef(const ColumnRefExpression &expr,
                                             const std::vector<std::shared_ptr<Operator>> &children);
  std::tuple<AggregateType, bool, std::shared_ptr<OperatorExpression>> PlanAggregate(
      const AggregateExpression &expr, const std::vector<std::shared_ptr<Operator>> &children);

  std::shared_ptr<Operator> PlanTableRef(const TableRef &ref, bool has_lock = false);
  std::shared_ptr<Operator> PlanBaseTable(const BaseTableRef &ref, bool has_lock);
  std::shared_ptr<Operator> PlanExpressionList(const ExpressionListRef &ref);
  std::shared_ptr<Operator> PlanCrossJoin(const CrossJoinRef &ref);
  std::shared_ptr<Operator> PlanJoin(const JoinRef &ref);

  std::shared_ptr<OperatorExpression> BinaryFactory(const std::string &op_name,
                                                    std::shared_ptr<OperatorExpression> left,
                                                    std::shared_ptr<OperatorExpression> right);

  std::shared_ptr<ColumnList> GetColumnList(const BaseTableRef &table) const;
  static std::shared_ptr<ColumnList> InferColumnList(const std::vector<std::shared_ptr<OperatorExpression>> &exprs);
  static std::shared_ptr<ColumnList> InferAggregateColumnList(
      const std::vector<std::shared_ptr<OperatorExpression>> &group_bys,
      const std::vector<std::shared_ptr<OperatorExpression>> &aggregates);
  static std::shared_ptr<ColumnList> GetJoinColumnList(const Operator &left, const Operator &right);
  static std::shared_ptr<ColumnList> RenameColumnList(std::shared_ptr<const ColumnList> column_list,
                                                      const std::vector<std::string> &col_names);

 private:
  ForceJoin force_join_;
  std::vector<std::shared_ptr<OperatorExpression>> aggregate_exprs_;
  size_t next_aggregate_ = 0;
  std::unordered_multimap<std::string, std::shared_ptr<OperatorExpression>> aliases_;
};

}  // namespace huadb
