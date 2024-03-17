#pragma once

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace fs = std::filesystem;

enum class RecordType { STATEMENT, QUERY };
enum class SortMode { NO_SORT, ROW_SORT, VALUE_SORT };
enum class ResultType { SUCCESS, ERROR };

class Location {
 public:
  std::string file_;
  size_t line_;
  friend std::ostream &operator<<(std::ostream &os, const Location &loc) {
    os << loc.file_ << ":" << loc.line_;
    return os;
  }
  std::string ToString() { return file_ + ":" + std::to_string(line_); }
};

class Record {
 public:
  Record(RecordType type, Location loc) : type_(type), loc_(std::move(loc)) {}
  virtual ~Record() = default;
  RecordType type_;
  Location loc_;
};

class StatementRecord : public Record {
 public:
  StatementRecord(Location loc, std::string sql, ResultType expected_result, std::string connection_name)
      : Record(RecordType::STATEMENT, std::move(loc)),
        sql_(std::move(sql)),
        expected_result_(expected_result),
        connection_name_(std::move(connection_name)) {}

  std::string sql_;
  ResultType expected_result_;
  std::string connection_name_;
};

class QueryRecord : public Record {
 public:
  QueryRecord(Location loc, std::string sql, SortMode sort_mode, std::string connection_name,
              std::string expected_result)
      : Record(RecordType::QUERY, std::move(loc)),
        sql_(std::move(sql)),
        sort_mode_(sort_mode),
        connection_name_(std::move(connection_name)),
        expected_result_(std::move(expected_result)) {}

  std::string sql_;
  SortMode sort_mode_;
  std::string connection_name_;
  std::string expected_result_;
};

class SQLLogicParser {
 public:
  bool OpenFile(fs::path path);
  void Parse();

  std::vector<std::unique_ptr<Record>> records;

 private:
  std::vector<std::string> Tokenize();

  fs::path path_;
  std::vector<std::string> lines_;
  std::vector<std::string>::const_iterator line_iter_;
};
