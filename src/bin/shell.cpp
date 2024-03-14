#include <filesystem>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include "common/constants.h"
#include "common/result_writer.h"
#include "database/connection.h"
#include "database/database_engine.h"
#include "linenoise.h"

namespace fs = std::filesystem;

void PlainShell() {
  std::string query;
  auto database = std::make_unique<huadb::DatabaseEngine>();
  auto connection = std::make_unique<huadb::Connection>(*database);
  while (std::getline(std::cin, query)) {
    try {
      if (query.substr(0, 5) == "crash") {
        database->Crash();
        std::cout << "CRASH" << std::endl;
      } else if (query.substr(0, 5) == "flush") {
        database->Flush();
        std::cout << "FLUSH" << std::endl;
      } else if (query.substr(0, 7) == "restart") {
        database.reset();
        database = std::make_unique<huadb::DatabaseEngine>();
        connection.reset();
        connection = std::make_unique<huadb::Connection>(*database);
        std::cout << "RESTART" << std::endl;
      } else {
        std::ostringstream result;
        auto writer = huadb::SimpleWriter(result);
        connection->SendQuery(query, writer);
        std::cout << result.str();
      }
    } catch (std::exception &e) {
      std::cerr << huadb::BOLD << huadb::RED << "Error: " << huadb::RESET << e.what() << std::endl;
    }
  }
}

void LinenoiseShell() {
  std::string history_file;
  auto *home_dir = getenv("HOME");
  if (home_dir != nullptr) {
    history_file = std::string(home_dir) + "/.huadb_history";
  } else {
    history_file = fs::absolute(".huadb_history");
  }
  linenoiseHistoryLoad(history_file.c_str());
  linenoiseHistorySetMaxLen(2048);
  linenoiseSetMultiLine(1);
  auto database = std::make_unique<huadb::DatabaseEngine>();
  auto connection = std::make_unique<huadb::Connection>(*database);
  while (true) {
    auto current_db = connection->GetCurrentDatabase();
    std::string in_transaction;
    if (connection->InTransaction()) {
      in_transaction = "*";
    }
    auto fresh_prompt = current_db + "=" + in_transaction + "> ";
    auto continue_prompt = current_db + "-" + in_transaction + "> ";
    std::string query;
    bool first_line = true;
    while (true) {
      auto line_prompt = first_line ? fresh_prompt : continue_prompt;
      auto *query_c_str = linenoise(line_prompt.c_str());
      if (query_c_str == nullptr || std::string(query_c_str) == "\\q") {
        linenoiseHistorySave(history_file.c_str());
        return;
      }
      query += query_c_str;
      if (!query.empty() && (query.back() == ';' || query[0] == '\\')) {
        break;
      }
      first_line = false;
      query += " ";
      linenoiseFree(query_c_str);
    }
    linenoiseHistoryAdd(query.c_str());
    try {
      if (query.substr(0, 5) == "crash") {
        database->Crash();
        std::cout << "CRASH" << std::endl;
      } else if (query.substr(0, 5) == "flush") {
        database->Flush();
        std::cout << "FLUSH" << std::endl;
      } else if (query.substr(0, 7) == "restart") {
        database.reset();
        database = std::make_unique<huadb::DatabaseEngine>();
        connection.reset();
        connection = std::make_unique<huadb::Connection>(*database);
        std::cout << "RESTART" << std::endl;
      } else {
        auto writer = huadb::FortWriter();
        connection->SendQuery(query, writer);
        for (const auto &table : writer.tables_) {
          std::cout << table;
        }
      }
    } catch (std::exception &e) {
      std::cerr << huadb::BOLD << huadb::RED << "Error: " << huadb::RESET << e.what() << std::endl;
    }
  }
  linenoiseHistorySave(history_file.c_str());
}

int main(int argc, char *argv[]) {
  std::cout << R"(Welcome to HuaDB. Type "\?" or "\h" for help.)" << std::endl;
  if (argc > 1 && strcmp(argv[1], "-s") == 0) {
    PlainShell();
  } else {
    LinenoiseShell();
  }
  return 0;
}
