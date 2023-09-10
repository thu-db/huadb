#pragma once

#include "operators/expressions/expression.h"

namespace huadb {

class TypeCast : public OperatorExpression {
 public:
  TypeCast(Type cast_type, std::shared_ptr<OperatorExpression> arg)
      : OperatorExpression(OperatorExpressionType::TYPE_CAST, {}, cast_type, "<no_name>"),
        cast_type_(cast_type),
        arg_(arg) {}
  Value Evaluate(std::shared_ptr<const Record> record) override {
    auto value = arg_->Evaluate(record);
    if (cast_type_ == Type::BOOL) {
      return value.CastAsBool();
    } else {
      throw DbException("Type unsupported for cast operation");
    }
  }
  Value EvaluateJoin(std::shared_ptr<const Record> left, std::shared_ptr<const Record> right) override {
    auto value = arg_->Evaluate(left);
    if (cast_type_ == Type::BOOL) {
      return value.CastAsBool();
    } else {
      throw DbException("Type unsupported for cast operation");
    }
  }
  std::string ToString() const override { return arg_->ToString(); }
  Type cast_type_;
  std::shared_ptr<OperatorExpression> arg_;
};

}  // namespace huadb
