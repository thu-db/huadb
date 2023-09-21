#pragma once

#include <string>
#include <vector>

namespace huadb {

class StringUtil {
 public:
  static void RTrim(std::string &str);
  static std::vector<std::string> Split(const std::string &str, char delim = '\n');
  static std::string Lower(const std::string &str);
  static std::string Upper(const std::string &str);
};

}  // namespace huadb
