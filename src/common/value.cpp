#include "common/value.h"

#include <cstring>
#include <sstream>

#include "common/exceptions.h"

namespace huadb {

Value::Value() : is_null_(true), type_(Type::NULL_TYPE), size_(0) {}

Value::Value(Type type, db_size_t size) : type_(type), size_(size), is_null_(true) {}

Value::Value(bool val) : type_(Type::BOOL), size_(TypeUtil::TypeSize(Type::BOOL)) { val_.bool_ = val; }

Value::Value(int32_t val) : type_(Type::INT), size_(TypeUtil::TypeSize(Type::INT)) { val_.int_ = val; }

Value::Value(uint32_t val) : type_(Type::UINT), size_(TypeUtil::TypeSize(Type::UINT)) { val_.uint_ = val; }

Value::Value(double val) : type_(Type::DOUBLE), size_(TypeUtil::TypeSize(Type::DOUBLE)) { val_.double_ = val; }

Value::Value(const char *val, Type type) : type_(type) {
  str_ = val;
  size_ = str_.size();
}

Value::Value(std::string val, Type type) : type_(type) {
  str_ = std::move(val);
  size_ = str_.size();
}

Value::Value(std::vector<Value> values) : values_(std::move(values)), type_(Type::LIST) {}

bool Value::IsNull() const { return is_null_ || type_ == Type::NULL_TYPE; }

db_size_t Value::GetSize() const { return size_; }

std::string Value::ToString() const {
  if (is_null_) {
    return "NULL";
  }
  switch (type_) {
    case Type::BOOL:
      if (val_.bool_) {
        return "true";
      } else {
        return "false";
      }
    case Type::INT:
      return std::to_string(val_.int_);
    case Type::UINT:
      return std::to_string(val_.uint_);
    case Type::DOUBLE: {
      std::ostringstream oss;
      oss << val_.double_;
      return oss.str();
    }
    case Type::CHAR:
    case Type::VARCHAR:
      return str_;
    default:
      throw DbException("Unknown value type in ToString");
  }
}

db_size_t Value::SerializeTo(char *data) const {
  auto result = size_;
  switch (type_) {
    case Type::BOOL:
      memcpy(data, &val_.bool_, size_);
      break;
    case Type::INT:
      memcpy(data, &val_.int_, size_);
      break;
    case Type::UINT:
      memcpy(data, &val_.uint_, size_);
      break;
    case Type::DOUBLE:
      memcpy(data, &val_.double_, size_);
      break;
    case Type::VARCHAR:
    case Type::CHAR: {
      db_size_t str_size = str_.size();
      memcpy(data, &str_size, 2);
      memcpy(data + 2, str_.c_str(), size_);
      result = str_size + 2;
      break;
    }
    default:
      throw DbException("Unknown value type in SerializeTo");
  }
  return result;
}

db_size_t Value::DeserializeFrom(const char *data) {
  is_null_ = false;
  auto result = size_;
  switch (type_) {
    case Type::BOOL:
      memcpy(&val_.bool_, data, size_);
      break;
    case Type::INT:
      memcpy(&val_.int_, data, size_);
      break;
    case Type::UINT:
      memcpy(&val_.uint_, data, size_);
      break;
    case Type::DOUBLE:
      memcpy(&val_.double_, data, size_);
      break;
    case Type::VARCHAR:
    case Type::CHAR: {
      memcpy(&size_, data, 2);
      str_ = std::string(data + 2, data + size_ + 2);
      result = size_ + 2;
      break;
    }
    default:
      throw DbException("Unknown value type in DeserializeFrom");
  }
  return result;
}

Type Value::GetType() const { return type_; }

const std::vector<Value> &Value::GetValues() const { return values_; }

template <>
bool Value::GetValue<bool>() const {
  if (type_ != Type::BOOL) {
    throw DbException("Type mismatch (expected bool)");
  }
  return val_.bool_;
}

template <>
int32_t Value::GetValue<int32_t>() const {
  if (type_ != Type::INT) {
    throw DbException("Type mismatch (expected int)");
  }
  return val_.int_;
}

template <>
uint32_t Value::GetValue<uint32_t>() const {
  if (type_ != Type::UINT) {
    throw DbException("Type mismatch (expected uint32_t)");
  }
  return val_.uint_;
}

template <>
double Value::GetValue<double>() const {
  if (type_ != Type::DOUBLE) {
    throw DbException("Type mismatch (expected double)");
  }
  return val_.double_;
}

template <>
std::string Value::GetValue<std::string>() const {
  if (!TypeUtil::IsString(type_)) {
    throw DbException("Type mismatch (expected char/varchar)");
  }
  return str_;
}

template <>
const char *Value::GetValue<const char *>() const {
  if (!TypeUtil::IsString(type_)) {
    throw DbException("Type mismatch (expected char/varchar)");
  }
  return str_.c_str();
}

bool Value::Less(const Value &other) const {
  if (type_ != other.type_) {
    throw DbException("Type mismatch (in Less)");
  }
  switch (type_) {
    case Type::INT:
      return val_.int_ < other.val_.int_;
    case Type::DOUBLE:
      return val_.double_ < other.val_.double_;
    case Type::CHAR:
    case Type::VARCHAR:
      return str_ < other.str_;
    default:
      throw DbException("Type unsupported for Less operation");
  }
}

bool Value::Equal(const Value &other) const {
  if (type_ != other.type_) {
    throw DbException("Type mismatch (in Equal)");
  }
  switch (type_) {
    case Type::BOOL:
      return val_.bool_ == other.val_.bool_;
    case Type::INT:
      return val_.int_ == other.val_.int_;
    case Type::DOUBLE:
      return val_.double_ == other.val_.double_;
    case Type::CHAR:
    case Type::VARCHAR:
      return str_ == other.str_;
    default:
      throw DbException("Type unsupported for Equal operation");
  }
}

bool Value::Greater(const Value &other) const {
  if (type_ != other.type_) {
    throw DbException("Type mismatch (in Greater)");
  }
  switch (type_) {
    case Type::INT:
      return val_.int_ > other.val_.int_;
    case Type::DOUBLE:
      return val_.double_ > other.val_.double_;
    case Type::CHAR:
    case Type::VARCHAR:
      return str_ > other.str_;
    default:
      throw DbException("Type unsupported for Greater operation");
  }
}

Value Value::Add(const Value &other) const {
  if (type_ != other.type_) {
    throw DbException("Type mismatch (in Add)");
  }
  switch (type_) {
    case Type::INT:
      return Value(val_.int_ + other.val_.int_);
    case Type::DOUBLE:
      return Value(val_.double_ + other.val_.double_);
    default:
      throw DbException("Type unsupported for Add operation");
  }
}

Value Value::Max(const Value &other) const {
  if (type_ != other.type_) {
    throw DbException("Type mismatch (in Max)");
  }
  switch (type_) {
    case Type::INT:
      return Value(std::max(val_.int_, other.val_.int_));
    case Type::DOUBLE:
      return Value(std::max(val_.double_, other.val_.double_));
    default:
      throw DbException("Type unsupported for Max operation");
  }
}

Value Value::Min(const Value &other) const {
  if (type_ != other.type_) {
    throw DbException("Type mismatch (in Min)");
  }
  switch (type_) {
    case Type::INT:
      return Value(std::min(val_.int_, other.val_.int_));
    case Type::DOUBLE:
      return Value(std::min(val_.double_, other.val_.double_));
    default:
      throw DbException("Type unsupported for Min operation");
  }
}

Value Value::Not() const {
  switch (type_) {
    case Type::BOOL:
      return Value(!val_.bool_);
    default:
      throw DbException("Type unsupported for Not operation");
  }
}

Value Value::CastAsBool() const {
  switch (type_) {
    case Type::BOOL:
      return Value(val_.bool_);
    case Type::CHAR:
    case Type::VARCHAR: {
      if (str_ == "t") {
        return Value(true);
      } else if (str_ == "f") {
        return Value(false);
      } else {
        throw DbException("Unknown str in CastAsBool: " + str_);
      }
    }
    default:
      throw DbException("Type unsupported for CastAsBool operation");
  }
}

bool Value::operator==(const Value &other) const { return Equal(other); }

}  // namespace huadb

namespace std {

uint64_t hash<huadb::Value>::operator()(const huadb::Value &other) const {
  switch (other.GetType()) {
    case huadb::Type::BOOL:
      return std::hash<bool>()(other.GetValue<bool>());
    case huadb::Type::INT:
      return std::hash<int32_t>()(other.GetValue<int32_t>());
    case huadb::Type::UINT:
      return std::hash<uint32_t>()(other.GetValue<uint32_t>());
    case huadb::Type::DOUBLE:
      return std::hash<double>()(other.GetValue<double>());
    case huadb::Type::VARCHAR:
    case huadb::Type::CHAR:
      return std::hash<std::string>()(other.GetValue<std::string>());
    default:
      throw huadb::DbException("Unknown value type in hash");
  }
};

}  // namespace std
