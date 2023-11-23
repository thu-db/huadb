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

#pragma once

#include <memory>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
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

  std::tuple<AggregateType, bool, std::shared_ptr<OperatorExpression>> GetAggregateType(
      const AggregateExpression &expr, const std::vector<std::shared_ptr<Operator>> &children);

  std::shared_ptr<Operator> PlanAggregate(const SelectStatement &stmt, std::shared_ptr<Operator> child);
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
  void AddAggregateExpression(Expression &expr);
  void CheckAggregate(const Expression &expr, const std::unordered_set<std::string> group_by_names);
  ForceJoin force_join_;
  std::vector<std::unique_ptr<Expression>> aggregates_;
  std::vector<std::shared_ptr<OperatorExpression>> aggregate_exprs_;
  size_t next_aggregate_ = 0;
  std::unordered_multimap<std::string, std::shared_ptr<OperatorExpression>> aliases_;
  std::unordered_map<std::string, std::string> alias2colname_;
};

}  // namespace huadb
