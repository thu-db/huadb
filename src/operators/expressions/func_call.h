#pragma once

#include "common/exceptions.h"
#include "common/string_util.h"
#include "common/type_util.h"
#include "fmt/ranges.h"
#include "operators/expressions/expression.h"

namespace huadb {

class FuncCall : public OperatorExpression {
 public:
  FuncCall(std::string function_name, std::vector<std::shared_ptr<OperatorExpression>> args)
      : OperatorExpression(OperatorExpressionType::FUNC_CALL, {}, GetReturnType(function_name), function_name),
        function_name_(std::move(function_name)),
        args_(std::move(args)) {
    if (function_name_ == "lower" || function_name_ == "upper" || function_name_ == "length") {
      if (args_.size() != 1 || !TypeUtil::IsString(args_[0]->GetValueType())) {
        throw DbException("Argument mismatch for function " + function_name_);
      }
    } else {
      throw std::runtime_error("Unknown function name " + function_name_);
    }
  }
  Value Evaluate(std::shared_ptr<const Record> record) override {
    if (function_name_ == "lower") {
      return Value(StringUtil::Lower(args_[0]->Evaluate(record).GetValue<std::string>()));
    } else if (function_name_ == "upper") {
      return Value(StringUtil::Upper(args_[0]->Evaluate(record).GetValue<std::string>()));
    } else if (function_name_ == "length") {
      return Value(static_cast<uint32_t>(args_[0]->Evaluate(record).GetValue<std::string>().size()));
    }
    throw std::runtime_error("Unknown function name " + function_name_);
  }
  Value EvaluateJoin(std::shared_ptr<const Record> left, std::shared_ptr<const Record> right) override {
    if (function_name_ == "lower") {
      return Value(StringUtil::Lower(args_[0]->EvaluateJoin(left, right).GetValue<std::string>()));
    } else if (function_name_ == "upper") {
      return Value(StringUtil::Upper(args_[0]->EvaluateJoin(left, right).GetValue<std::string>()));
    } else if (function_name_ == "length") {
      return Value(static_cast<uint32_t>(args_[0]->EvaluateJoin(left, right).GetValue<std::string>().size()));
    }
    throw std::runtime_error("Unknown function name " + function_name_);
  }
  std::string ToString() const override { return fmt::format("{}({})", function_name_, args_); }
  std::string function_name_;
  std::vector<std::shared_ptr<OperatorExpression>> args_;

 private:
  Type GetReturnType(const std::string &function_name) {
    if (function_name == "lower") {
      return Type::VARCHAR;
    } else if (function_name == "upper") {
      return Type::VARCHAR;
    } else if (function_name == "length") {
      return Type::INT;
    } else {
      throw std::runtime_error("Unknown function name " + function_name);
    }
  }
};

}  // namespace huadb
