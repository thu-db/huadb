#pragma once

#include <string>

namespace huadb {

enum class Type { BOOL, INT, UINT, DOUBLE, CHAR, VARCHAR, NULL_TYPE, LIST };

class TypeUtil {
 public:
  static std::string Type2String(Type type);
  static Type String2Type(const std::string &str);
  static size_t TypeSize(Type type);
  static bool IsNumeric(Type type);
  static bool IsString(Type type);
  static bool TypeCompatible(Type type1, Type type2);
};

}  // namespace huadb
