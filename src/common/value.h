#pragma once

#include <functional>
#include <string>
#include <vector>

#include "common/type_util.h"
#include "common/types.h"

namespace huadb {

class Value {
 public:
  Value();
  Value(Type type, db_size_t size);
  explicit Value(bool val);
  explicit Value(int32_t val);
  explicit Value(uint32_t val);
  explicit Value(double val);
  explicit Value(const char *val, Type type = Type::VARCHAR);
  explicit Value(std::string val, Type type = Type::VARCHAR);
  explicit Value(std::vector<Value> values);
  bool IsNull() const;
  db_size_t GetSize() const;
  std::string ToString() const;
  db_size_t SerializeTo(char *data) const;
  db_size_t DeserializeFrom(const char *data);

  Type GetType() const;

  const std::vector<Value> &GetValues() const;

  template <typename T>
  T GetValue() const;

  bool Less(const Value &other) const;
  bool Equal(const Value &other) const;
  bool Greater(const Value &other) const;

  Value Add(const Value &other) const;
  Value Max(const Value &other) const;
  Value Min(const Value &other) const;

  Value Not() const;

  Value CastAsBool() const;

  bool operator==(const Value &other) const;

 private:
  Type type_;
  bool is_null_ = false;
  union {
    bool bool_;
    int32_t int_;
    uint32_t uint_;
    uint64_t uint64_;
    double double_;
  } val_;
  std::string str_;
  std::vector<Value> values_;
  db_size_t size_;
};

}  // namespace huadb

namespace std {

template <>
struct hash<huadb::Value> {
  uint64_t operator()(const huadb::Value &other) const;
};

}  // namespace std
