#pragma once

#include <stdexcept>
#include <string>

namespace huadb {

class DbException : public std::exception {
 public:
  explicit DbException(const std::string &message) { message_ = message; }
  const char *what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
};

}  // namespace huadb
