#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "argparse/argparse.hpp"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/result_writer.h"
#include "common/string_util.h"
#include "database/connection.h"
#include "database/database_engine.h"
#include "sqllogicparser.h"

static constexpr const char *TEST_DIRECTORY = "huadb_test";

std::unordered_map<std::string, std::unique_ptr<huadb::Connection>> connections;

bool CompareResult(const std::string &result, const std::string &expected_result, SortMode sort_mode,
                   std::ostringstream &error_stream) {
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
    std::ofstream result_stream("../yours.log");
    for (const auto &line : result_lines) {
      result_stream << line << std::endl;
    }

    error_stream << "Expected Result:" << std::endl;
    error_stream << expected_result;
    std::ofstream expected_stream("../expected.log");
    for (const auto &line : expected_lines) {
      expected_stream << line << std::endl;
    }
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
        std::ostringstream result;
        auto writer = huadb::SimpleWriter(result, true);
        try {
          if (connections.find(statement.connection_name_) == connections.end()) {
            connections[statement.connection_name_] = std::make_unique<huadb::Connection>(*database);
          }
          if (statement.sql_.substr(0, 5) == "crash") {
            database->Crash();
          } else if (statement.sql_.substr(0, 5) == "flush") {
            database->Flush();
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
            std::cerr << huadb::BOLD << huadb::RED << "ERROR\n"
                      << huadb::RESET << record->loc_ << "\nUnexpected error: " << e.what() << std::endl;
            return false;
          }
        }
        break;
      }
      case RecordType::QUERY: {
        const auto &query = dynamic_cast<const QueryRecord &>(*record);
        std::ostringstream result;
        auto writer = huadb::SimpleWriter(result, true);
        try {
          if (connections.find(query.connection_name_) == connections.end()) {
            connections[query.connection_name_] = std::make_unique<huadb::Connection>(*database);
          }
          connections[query.connection_name_]->SendQuery(query.sql_, writer);
          std::ostringstream error_stream;
          if (!CompareResult(result.str(), query.expected_result_, query.sort_mode_, error_stream)) {
            std::cerr << huadb::BOLD << huadb::RED << "ERROR\n"
                      << huadb::RESET << record->loc_ << "\nUnexpected error: Wrong Result\n"
                      << error_stream.str() << std::endl;
            return false;
          }
        } catch (huadb::DbException &e) {
          std::cerr << huadb::BOLD << huadb::RED << "ERROR\n"
                    << huadb::RESET << record->loc_ << "\nUnexpected error: " << e.what() << std::endl;
          return false;
        }
        break;
      }
      default: {
        std::cerr << huadb::BOLD << huadb::RED << "ERROR\n" << huadb::RESET << "Unknown record type" << std::endl;
        return false;
      }
    }
  }
  return true;
}

void ReportResult(const std::vector<std::string> &success_cases, const std::vector<std::string> &fail_cases) {
  std::ofstream report("report.json");
  report << "{\"success_cases\": [";
  for (size_t i = 0; i < success_cases.size(); i++) {
    report << "\"" << success_cases[i] << "\"";
    if (i != success_cases.size() - 1) {
      report << ", ";
    }
  }
  report << "], \"fail_cases\": [";
  for (size_t i = 0; i < fail_cases.size(); i++) {
    report << "\"" << fail_cases[i] << "\"";
    if (i != fail_cases.size() - 1) {
      report << ", ";
    }
  }
  report << "]}";
}

int main(int argc, char *argv[]) {
  argparse::ArgumentParser program("sqllogictest");
  program.add_argument("-c", "--count")
      .help("Number of times to run the tests")
      .default_value(1u)
      .metavar("COUNT")
      .scan<'u', unsigned>();
  program.add_argument("-o", "--output").help("Report the result of the test").flag();
  program.add_argument("test_files").help("Test files to run").nargs(argparse::nargs_pattern::at_least_one);

  try {
    program.parse_args(argc, argv);
  } catch (const std::exception &err) {
    std::cerr << err.what() << std::endl;
    std::cerr << program;
    std::exit(1);
  }

  bool report_result = program.get<bool>("-o");
  int test_count = program.get<unsigned>("-c");
  auto test_files = program.get<std::vector<std::string>>("test_files");

  std::vector<fs::path> paths;
  for (const auto &file : test_files) {
    paths.emplace_back(fs::absolute(file));
  }

  bool success = true;
  std::vector<std::string> success_cases;
  std::vector<std::string> fail_cases;
  for (unsigned i = 0; i < test_count; i++) {
    success_cases.clear();
    fail_cases.clear();
    if (fs::is_directory(TEST_DIRECTORY)) {
      fs::remove_all(TEST_DIRECTORY);
    }
    fs::create_directory(TEST_DIRECTORY);
    fs::current_path(TEST_DIRECTORY);

    for (const auto &path : paths) {
      auto testcase_name = path.parent_path().filename().string() + "/" + path.filename().string();
      std::cout << testcase_name << " " << std::flush;
      try {
        bool passed = Run(path);
        success &= passed;
        if (!passed) {
          fail_cases.push_back(testcase_name);
          continue;
        }
      } catch (std::exception &e) {
        std::cerr << huadb::BOLD << huadb::RED << "ERROR\n" << huadb::RESET << e.what() << std::endl;
        success = false;
        fail_cases.push_back(testcase_name);
        continue;
      }
      std::cout << huadb::BOLD << huadb::GREEN << "PASS" << huadb::RESET << std::endl;
      success_cases.push_back(testcase_name);
    }
    fs::current_path("..");
    if (!success) {
      break;
    }
  }

  if (report_result) {
    ReportResult(success_cases, fail_cases);
  }

  if (success) {
    return 0;
  } else {
    return 1;
  }
}
