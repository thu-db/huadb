#include "common/string_util.h"

#include <algorithm>
#include <sstream>

namespace huadb {

void StringUtil::RTrim(std::string &str) {
  str.erase(std::find_if(str.rbegin(), str.rend(), [](int character) { return std::isspace(character) == 0; }).base(),
            str.end());
}

std::vector<std::string> StringUtil::Split(const std::string &str, char delim) {
  std::istringstream iss(str);
  std::vector<std::string> result;
  std::string token;
  while (std::getline(iss, token, delim)) {
    RTrim(token);
    result.push_back(token);
  }
  return result;
}

std::string StringUtil::Lower(const std::string &str) {
  std::string result = str;
  std::transform(result.begin(), result.end(), result.begin(), ::tolower);
  return result;
}

std::string StringUtil::Upper(const std::string &str) {
  std::string result = str;
  std::transform(result.begin(), result.end(), result.begin(), ::toupper);
  return result;
}

}  // namespace huadb
