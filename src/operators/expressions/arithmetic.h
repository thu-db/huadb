#pragma once

#include "fmt/format.h"
#include "operators/expressions/expression.h"

namespace huadb {

enum class ArithmeticType { ADD, SUB, MUL, DIV };

class Arithmetic : public OperatorExpression {
 public:
  Arithmetic(ArithmeticType type, std::shared_ptr<OperatorExpression> left, std::shared_ptr<OperatorExpression> right)
      : OperatorExpression(OperatorExpressionType::ARITHMETIC, {std::move(left), std::move(right)}, Type::INT),
        type_(type) {}

  Value Evaluate(std::shared_ptr<const Record> record) override {
    Value lhs = children_[0]->Evaluate(record);
    Value rhs = children_[1]->Evaluate(record);
    return Compute(lhs, rhs);
  }

  Value EvaluateJoin(std::shared_ptr<const Record> left, std::shared_ptr<const Record> right) override {
    Value lhs = children_[0]->EvaluateJoin(left, right);
    Value rhs = children_[1]->EvaluateJoin(left, right);
    return Compute(lhs, rhs);
  }

  std::string ToString() const override { return fmt::format("{} {} {}", children_[0], type_, children_[1]); }

 private:
  ArithmeticType type_;
  Value Compute(const Value &lhs, const Value &rhs) {
    if (lhs.IsNull() || rhs.IsNull()) {
      return Value();
    }
    switch (lhs.GetType()) {
      case Type::INT:
        return Value(DoOperation(lhs.GetValue<int32_t>(), rhs.GetValue<int32_t>()));
      case Type::DOUBLE:
        return Value(DoOperation(lhs.GetValue<double>(), rhs.GetValue<double>()));
      default:
        throw DbException("Type unsupported for arithmetic operation");
    }
  }

  template <typename T>
  T DoOperation(T lhs, T rhs) {
    switch (type_) {
      case ArithmeticType::ADD:
        return lhs + rhs;
      case ArithmeticType::SUB:
        return lhs - rhs;
      case ArithmeticType::MUL:
        return lhs * rhs;
      case ArithmeticType::DIV:
        return lhs / rhs;
      default:
        throw DbException("Unknown arithmetic type");
    }
  }
};

}  // namespace huadb

template <>
struct fmt::formatter<huadb::ArithmeticType> : formatter<string_view> {
  auto format(huadb::ArithmeticType type, format_context &ctx) const {
    string_view name = "unknown";
    switch (type) {
      case huadb::ArithmeticType::ADD:
        name = "+";
        break;
      case huadb::ArithmeticType::SUB:
        name = "-";
        break;
      case huadb::ArithmeticType::MUL:
        name = "*";
        break;
      case huadb::ArithmeticType::DIV:
        name = "/";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
