#include "common/type_util.h"

#include "common/exceptions.h"

namespace huadb {

std::string TypeUtil::Type2String(Type type) {
  switch (type) {
    case Type::BOOL:
      return "bool";
    case Type::INT:
      return "int";
    case Type::UINT:
      return "uint";
    case Type::DOUBLE:
      return "double";
    case Type::CHAR:
      return "char";
    case Type::VARCHAR:
      return "varchar";
    case Type::NULL_TYPE:
      return "null";
    default:
      throw DbException("Unknown type in Type2String");
  }
}

Type TypeUtil::String2Type(const std::string &str) {
  if (str == "bool") {
    return Type::BOOL;
  } else if (str == "int") {
    return Type::INT;
  } else if (str == "uint") {
    return Type::UINT;
  } else if (str == "double") {
    return Type::DOUBLE;
  } else if (str == "char") {
    return Type::CHAR;
  } else if (str == "varchar") {
    return Type::VARCHAR;
  } else if (str == "null") {
    return Type::NULL_TYPE;
  } else {
    throw DbException("Unknown type in String2Type");
  }
}

size_t TypeUtil::TypeSize(Type type) {
  switch (type) {
    case Type::NULL_TYPE:
      return 0;
    case Type::BOOL:
      return 1;
    case Type::INT:
    case Type::UINT:
      return 4;
    case Type::DOUBLE:
      return 8;
    case Type::CHAR:
    case Type::VARCHAR:
      throw DbException("Size must be specified for CHAR and VARCHAR types");
    default:
      throw DbException("Unknown type in TypeSize");
  }
}

bool TypeUtil::IsNumeric(Type type) {
  switch (type) {
    case Type::INT:
    case Type::UINT:
    case Type::DOUBLE:
      return true;
    default:
      return false;
  }
}

bool TypeUtil::IsString(Type type) {
  switch (type) {
    case Type::CHAR:
    case Type::VARCHAR:
      return true;
    default:
      return false;
  }
}

bool TypeUtil::TypeCompatible(Type type1, Type type2) {
  if (IsNumeric(type1) && IsNumeric(type2)) {
    return true;
  }
  if (IsString(type1) && IsString(type2)) {
    return true;
  }
  return type1 == type2;
}

}  // namespace huadb
