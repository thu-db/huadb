#pragma once

#include <string>

namespace huadb {

class ResultWriter;
class DatabaseEngine;

class Connection {
 public:
  explicit Connection(DatabaseEngine &database);
  void SendQuery(const std::string &sql, ResultWriter &writer) const;
  std::string GetCurrentDatabase() const;
  bool InTransaction() const;
  void Rollback() const;

 private:
  DatabaseEngine &database_;
};

}  // namespace huadb
