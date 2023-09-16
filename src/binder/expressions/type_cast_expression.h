#pragma once

#include "binder/expression.h"
#include "common/type_util.h"
#include "fmt/format.h"

namespace huadb {

class TypeCastExpression : public Expression {
 public:
  TypeCastExpression(Type cast_type, std::unique_ptr<Expression> arg)
      : Expression(ExpressionType::TYPE_CAST), cast_type_(cast_type), arg_(std::move(arg)) {}
  std::string ToString() const override { return fmt::format("Cast {}: {}", TypeUtil::Type2String(cast_type_), arg_); }
  bool HasAggregation() const override { return false; }
  Type cast_type_;
  std::unique_ptr<Expression> arg_;
};

}  // namespace huadb
