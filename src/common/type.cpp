#include "common/type.h"

#include "common/exceptions.h"

namespace huadb {

std::string Type2String(Type type) {
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

Type String2Type(const std::string &str) {
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

size_t TypeSize(Type type) {
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

}  // namespace huadb
