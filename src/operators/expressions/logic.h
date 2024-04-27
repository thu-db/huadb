#pragma once

#include <cassert>

#include "common/exceptions.h"
#include "fmt/format.h"
#include "operators/expressions/expression.h"

namespace huadb {

enum class LogicType { AND, OR, NOT };

class Logic : public OperatorExpression {
 public:
  Logic(LogicType logic_type, std::shared_ptr<OperatorExpression> arg)
      : OperatorExpression(OperatorExpressionType::LOGIC, {std::move(arg)}, Type::BOOL), logic_type_(logic_type) {
    assert(logic_type == LogicType::NOT);
  }

  Logic(LogicType logic_type, std::shared_ptr<OperatorExpression> left, std::shared_ptr<OperatorExpression> right)
      : OperatorExpression(OperatorExpressionType::LOGIC, {std::move(left), std::move(right)}, Type::BOOL),
        logic_type_(logic_type) {}

  Value Evaluate(std::shared_ptr<const Record> record) override {
    if (logic_type_ == LogicType::NOT) {
      return children_[0]->Evaluate(record).Not();
    } else {
      Value lhs = children_[0]->Evaluate(record);
      Value rhs = children_[1]->Evaluate(record);
      return Compute(lhs, rhs);
    }
  }

  Value EvaluateJoin(std::shared_ptr<const Record> left, std::shared_ptr<const Record> right) override {
    if (logic_type_ == LogicType::NOT) {
      return children_[0]->Evaluate(left).Not();
    } else {
      Value lhs = children_[0]->EvaluateJoin(left, right);
      Value rhs = children_[1]->EvaluateJoin(left, right);
      return Compute(lhs, rhs);
    }
  }

  std::string ToString() const override {
    if (logic_type_ == LogicType::NOT) {
      return fmt::format("{} {}", logic_type_, children_[0]);
    }
    return fmt::format("{} {} {}", children_[0], logic_type_, children_[1]);
  }
  LogicType GetLogicType() const { return logic_type_; }

 private:
  LogicType logic_type_;
  Value Compute(const Value &lhs, const Value &rhs) {
    if (lhs.IsNull() || rhs.IsNull()) {
      return Value();
    }
    switch (lhs.GetType()) {
      case Type::BOOL:
        return Value(DoOperation(lhs.GetValue<bool>(), rhs.GetValue<bool>()));
      default:
        throw DbException("Type unsupported for logic operation");
    }
  }

  bool DoOperation(bool lhs, bool rhs) {
    switch (logic_type_) {
      case LogicType::AND:
        return lhs && rhs;
      case LogicType::OR:
        return lhs || rhs;
      default:
        throw DbException("Unknown logic type");
    }
  }
};

}  // namespace huadb

template <>
struct fmt::formatter<huadb::LogicType> : formatter<string_view> {
  auto format(huadb::LogicType type, format_context &ctx) const {
    string_view name = "unknown";
    switch (type) {
      case huadb::LogicType::AND:
        name = "and";
        break;
      case huadb::LogicType::OR:
        name = "or";
        break;
      case huadb::LogicType::NOT:
        name = "not";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
