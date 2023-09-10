#pragma once

#include <string>

namespace huadb {

enum class Type { BOOL, INT, UINT, DOUBLE, CHAR, VARCHAR, NULL_TYPE, LIST };

std::string Type2String(Type type);

Type String2Type(const std::string &str);

size_t TypeSize(Type type);

}  // namespace huadb
