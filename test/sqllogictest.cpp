#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/constants.h"
#include "common/exceptions.h"
#include "common/result_writer.h"
#include "common/string_util.h"
#include "database/connection.h"
#include "database/database_engine.h"
#include "sqllogicparser.h"

static constexpr size_t TEST_COUNT = 3;
static constexpr const char *TEST_DIRECTORY = "huadb_test";

std::unordered_map<std::string, std::unique_ptr<huadb::Connection>> connections;

bool CompareResult(const std::string &result, const std::string &expected_result, SortMode sort_mode,
                   std::stringstream &error_stream) {
  auto result_lines = huadb::StringUtil::Split(result, '\n');
  auto expected_lines = huadb::StringUtil::Split(expected_result, '\n');
  if (sort_mode == SortMode::ROW_SORT) {
    std::sort(result_lines.begin(), result_lines.end());
    std::sort(expected_lines.begin(), expected_lines.end());
  }
  bool correct = (result_lines == expected_lines);
  if (!correct) {
    error_stream << "Your Result:" << std::endl;
    error_stream << result << std::endl;
    std::fstream result_stream("../yours.log", std::fstream::out | std::fstream::trunc);
    for (const auto &line : result_lines) {
      result_stream << line << std::endl;
    }
    result_stream.close();

    error_stream << "Expected Result:" << std::endl;
    error_stream << expected_result;
    std::fstream expected_stream("../expected.log", std::fstream::out | std::fstream::trunc);
    for (const auto &line : expected_lines) {
      expected_stream << line << std::endl;
    }
    expected_stream.close();
  }
  return correct;
}

bool Run(const fs::path &path) {
  SQLLogicParser parser;
  bool success = parser.OpenFile(path);
  if (!success) {
    std::cerr << "Cannot open test script " << path << std::endl;
    exit(1);
  }
  parser.Parse();
  connections.clear();
  auto database = std::make_unique<huadb::DatabaseEngine>();

  for (const auto &record : parser.records) {
    switch (record->type_) {
      case RecordType::STATEMENT: {
        const auto &statement = dynamic_cast<const StatementRecord &>(*record);
        std::stringstream result;
        auto writer = huadb::SimpleWriter(result, true);
        try {
          if (connections.find(statement.connection_name_) == connections.end()) {
            connections[statement.connection_name_] = std::make_unique<huadb::Connection>(*database);
          }
          if (statement.sql_.substr(0, 5) == "crash") {
            database->Crash();
          } else if (statement.sql_.substr(0, 7) == "restart") {
            database.reset();
            database = std::make_unique<huadb::DatabaseEngine>();
            connections.clear();
          } else {
            connections[statement.connection_name_]->SendQuery(statement.sql_, writer);
            if (statement.expected_result_ == ResultType::ERROR) {
              std::cerr << huadb::BOLD << huadb::RED << "ERROR\n"
                        << huadb::RESET << record->loc_ << "\nUnexpected success" << std::endl;
              return false;
            }
          }
        } catch (huadb::DbException &e) {
          if (statement.expected_result_ == ResultType::SUCCESS) {
            throw huadb::DbException(record->loc_.ToString() + "\nUnexpected error: " + e.what());
          }
        }
        break;
      }
      case RecordType::QUERY: {
        const auto &query = dynamic_cast<const QueryRecord &>(*record);
        std::stringstream result;
        auto writer = huadb::SimpleWriter(result, true);
        try {
          if (connections.find(query.connection_name_) == connections.end()) {
            connections[query.connection_name_] = std::make_unique<huadb::Connection>(*database);
          }
          connections[query.connection_name_]->SendQuery(query.sql_, writer);
          std::stringstream error_stream;
          if (!CompareResult(result.str(), query.expected_result_, query.sort_mode_, error_stream)) {
            throw huadb::DbException("Wrong Result\n" + error_stream.str());
          }
        } catch (huadb::DbException &e) {
          throw huadb::DbException(record->loc_.ToString() + "\nUnexpected error: " + e.what());
        }
        break;
      }
      default: {
        throw huadb::DbException("Unknown record type");
      }
    }
  }
  return true;
}

int main(int argc, char *argv[]) {
  std::vector<fs::path> paths;
  for (int i = 1; i < argc; i++) {
    paths.emplace_back(fs::absolute(argv[i]));
  }

  bool success = true;
  for (size_t i = 0; i < TEST_COUNT; i++) {
    std::cout << "Test: " << i + 1 << "/" << TEST_COUNT << std::endl;

    if (fs::is_directory(TEST_DIRECTORY)) {
      fs::remove_all(TEST_DIRECTORY);
    }
    fs::create_directory(TEST_DIRECTORY);
    fs::current_path(TEST_DIRECTORY);

    for (const auto &path : paths) {
      std::cout << path.parent_path().filename().string() << "/" << path.filename().string() << " " << std::flush;
      try {
        bool passed = Run(path);
        success &= passed;
        if (!passed) {
          continue;
        }
      } catch (std::exception &e) {
        std::cerr << huadb::BOLD << huadb::RED << "ERROR\n" << huadb::RESET << e.what() << std::endl;
        success = false;
        continue;
      }
      std::cout << huadb::BOLD << huadb::GREEN << "PASS" << huadb::RESET << std::endl;
    }
    fs::current_path("..");
    if (!success) {
      break;
    }
  }
  if (success) {
    return 0;
  } else {
    return 1;
  }
}
