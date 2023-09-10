#include "common/string_util.h"

#include <algorithm>
#include <sstream>

namespace huadb {

void StringUtil::RTrim(std::string &str) {
  str.erase(std::find_if(str.rbegin(), str.rend(), [](int character) { return std::isspace(character) == 0; }).base(),
            str.end());
}

std::vector<std::string> StringUtil::Split(const std::string &str, char delim) {
  std::stringstream ss(str);
  std::vector<std::string> result;
  std::string token;
  while (std::getline(ss, token, delim)) {
    RTrim(token);
    result.push_back(token);
  }
  return result;
}

}  // namespace huadb
