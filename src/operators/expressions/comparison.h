#pragma once

#include <codecvt>
#include <locale>
#include <regex>

#include "common/exceptions.h"
#include "fmt/format.h"
#include "operators/expressions/expression.h"

namespace huadb {

enum class ComparisonType {
  EQUAL,
  NOT_EQUAL,
  LESS,
  LESS_EQUAL,
  GREATER,
  GREATER_EQUAL,
  BETWEEN,
  NOT_BETWEEN,
  IN,
  NOT_IN,
  LIKE,
  NOT_LIKE
};

class Comparison : public OperatorExpression {
 public:
  Comparison(ComparisonType type, std::shared_ptr<OperatorExpression> left, std::shared_ptr<OperatorExpression> right)
      : OperatorExpression(OperatorExpressionType::COMPARISON, {std::move(left), std::move(right)}, Type::BOOL),
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
  ComparisonType GetComparisonType() { return type_; }

 private:
  ComparisonType type_;
  Value Compute(const Value &lhs, const Value &rhs) {
    if (lhs.IsNull() || rhs.IsNull()) {
      return Value();
    }
    if (type_ == ComparisonType::BETWEEN || type_ == ComparisonType::NOT_BETWEEN) {
      bool between = false;
      switch (lhs.GetType()) {
        case Type::INT:
          between = (lhs.GetValue<int32_t>() >= rhs.GetValues()[0].GetValue<int32_t>()) &&
                    (lhs.GetValue<int32_t>() <= rhs.GetValues()[1].GetValue<int32_t>());
          break;
        case Type::DOUBLE:
          between = (lhs.GetValue<double>() >= rhs.GetValues()[0].GetValue<double>()) &&
                    (lhs.GetValue<double>() <= rhs.GetValues()[1].GetValue<double>());
          break;
        default:
          throw DbException("Type unsupported for comparison operation (between)");
      }
      if (type_ == ComparisonType::BETWEEN) {
        return Value(between);
      } else if (type_ == ComparisonType::NOT_BETWEEN) {
        return Value(!between);
      } else {
        throw DbException("Unreachable code");
      }
    } else if (type_ == ComparisonType::IN || type_ == ComparisonType::NOT_IN) {
      bool in_list = false;
      for (const auto &value : rhs.GetValues()) {
        switch (lhs.GetType()) {
          case Type::INT:
            in_list = lhs.GetValue<int32_t>() == value.GetValue<int32_t>();
            break;
          case Type::DOUBLE:
            in_list = lhs.GetValue<double>() == value.GetValue<double>();
            break;
          case Type::CHAR:
          case Type::VARCHAR:
            in_list = lhs.GetValue<std::string>() == value.GetValue<std::string>();
            break;
          default:
            throw DbException("Type unsupported for comparison operation (in)");
        }
        if (in_list) {
          break;
        }
      }
      if (type_ == ComparisonType::IN) {
        return Value(in_list);
      } else if (type_ == ComparisonType::NOT_IN) {
        return Value(!in_list);
      } else {
        throw DbException("Unreachable code");
      }
    } else if (type_ == ComparisonType::LIKE || type_ == ComparisonType::NOT_LIKE) {
      if (!TypeUtil::IsString(lhs.GetType()) || !TypeUtil::IsString(rhs.GetType())) {
        throw DbException("LIKE operator only supports CHAR and VARCHAR types");
      }
      // Inefficient implementation of LIKE operator with Chinese support
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
      std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
#pragma clang diagnostic pop
      std::string pattern_string = std::regex_replace(rhs.GetValue<std::string>(), std::regex("%"), ".*");
      pattern_string = std::regex_replace(pattern_string, std::regex("_"), ".");
      std::wstring w_pattern_string = converter.from_bytes(pattern_string);
      std::wregex pattern(w_pattern_string);

      bool matched = std::regex_match(converter.from_bytes(lhs.GetValue<std::string>()), pattern);
      if (type_ == ComparisonType::LIKE) {
        return Value(matched);
      } else if (type_ == ComparisonType::NOT_LIKE) {
        return Value(!matched);
      } else {
        throw DbException("Unreachable code");
      }
    } else {
      switch (lhs.GetType()) {
        case Type::INT:
          if (rhs.GetType() == Type::INT) {
            return Value(DoOperation(lhs.GetValue<int32_t>(), rhs.GetValue<int32_t>()));
          } else if (rhs.GetType() == Type::DOUBLE) {
            return Value(DoOperation(lhs.GetValue<int32_t>(), rhs.GetValue<double>()));
          } else {
            throw DbException("Type unsupported for comparison operation");
          }
        case Type::DOUBLE:
          if (rhs.GetType() == Type::INT) {
            return Value(DoOperation(lhs.GetValue<double>(), rhs.GetValue<int32_t>()));
          } else if (rhs.GetType() == Type::DOUBLE) {
            return Value(DoOperation(lhs.GetValue<double>(), rhs.GetValue<double>()));
          } else {
            throw DbException("Type unsupported for comparison operation");
          }
        case Type::CHAR:
        case Type::VARCHAR:
          return Value(DoOperation(lhs.GetValue<std::string>(), rhs.GetValue<std::string>()));
        default:
          throw DbException("Type unsupported for comparison operation");
      }
    }
  }

  template <typename T, typename U>
  bool DoOperation(T lhs, U rhs) {
    switch (type_) {
      case ComparisonType::EQUAL:
        return lhs == rhs;
      case ComparisonType::NOT_EQUAL:
        return lhs != rhs;
      case ComparisonType::LESS:
        return lhs < rhs;
      case ComparisonType::LESS_EQUAL:
        return lhs <= rhs;
      case ComparisonType::GREATER:
        return lhs > rhs;
      case ComparisonType::GREATER_EQUAL:
        return lhs >= rhs;
      default:
        throw DbException("Unknown comparison type");
    }
  }
};

}  // namespace huadb

template <>
struct fmt::formatter<huadb::ComparisonType> : formatter<string_view> {
  auto format(huadb::ComparisonType type, format_context &ctx) const {
    string_view name = "unknown";
    switch (type) {
      case huadb::ComparisonType::EQUAL:
        name = "=";
        break;
      case huadb::ComparisonType::NOT_EQUAL:
        name = "!=";
        break;
      case huadb::ComparisonType::LESS:
        name = "<";
        break;
      case huadb::ComparisonType::LESS_EQUAL:
        name = "<=";
        break;
      case huadb::ComparisonType::GREATER:
        name = ">";
        break;
      case huadb::ComparisonType::GREATER_EQUAL:
        name = ">=";
        break;
      case huadb::ComparisonType::BETWEEN:
        name = "between";
        break;
      case huadb::ComparisonType::NOT_BETWEEN:
        name = "not between";
        break;
      case huadb::ComparisonType::IN:
        name = "in";
        break;
      case huadb::ComparisonType::NOT_IN:
        name = "not_in";
        break;
      case huadb::ComparisonType::LIKE:
        name = "~~";
        break;
      case huadb::ComparisonType::NOT_LIKE:
        name = "!~~";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
