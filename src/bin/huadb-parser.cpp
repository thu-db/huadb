#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include "argparse/argparse.hpp"
#include "common/constants.h"
#include "table/table_page.h"

namespace fs = std::filesystem;

enum class ParseMode { CONTROL, DATA, LOG };

void parse_control(const fs::path &path) {
  auto filename = path / huadb::CONTROL_NAME;
  if (!fs::is_regular_file(filename)) {
    std::cerr << "File not found: " << filename << std::endl;
    std::exit(1);
  }
  std::ifstream file(filename);
  if (file.fail()) {
    std::cerr << "Failed to open file: " << filename << std::endl;
    std::exit(1);
  }
  huadb::xid_t xid;
  huadb::lsn_t lsn;
  huadb::oid_t oid;
  bool normal_shutdown;
  file >> xid >> lsn >> oid >> normal_shutdown;
  std::cout << "next xid: " << xid << std::endl;
  std::cout << "next lsn: " << lsn << std::endl;
  std::cout << "next oid: " << oid << std::endl;
  std::cout << "normal_shutdown: " << normal_shutdown << std::endl;
}

void parse_data(const fs::path &path) {
  if (!fs::is_regular_file(path)) {
    std::cerr << "File not found: " << path << std::endl;
    std::exit(1);
  }
  std::ifstream file(path, std::fstream::binary);
  if (file.fail()) {
    std::cerr << "Failed to open file: " << path << std::endl;
    std::exit(1);
  }
  char buffer[huadb::DB_PAGE_SIZE];
  huadb::pageid_t page_id = 0;
  while (!file.eof()) {
    file.read(buffer, huadb::DB_PAGE_SIZE);
    if (file.gcount() == 0) {
      break;
    }
    if (file.gcount() != huadb::DB_PAGE_SIZE) {
      std::cerr << "Incorrect page size" << std::endl;
      std::exit(1);
    }
    auto page = std::make_unique<huadb::Page>();
    memcpy(page->GetData(), buffer, huadb::DB_PAGE_SIZE);
    huadb::TablePage table_page(std::move(page));
    std::cout << "page id: " << page_id << std::endl;
    std::cout << table_page.ToString() << std::endl;
    page_id++;
  }
}

void parse_log(const fs::path &path) {
  auto next_lsn_name = path / huadb::NEXT_LSN_NAME;
  auto log_name = path / huadb::LOG_NAME;
  if (!fs::is_regular_file(next_lsn_name)) {
    std::cerr << "File not found: " << next_lsn_name << std::endl;
    std::exit(1);
  }
  std::ifstream file(next_lsn_name);
  if (file.fail()) {
    std::cerr << "Failed to open file: " << next_lsn_name << std::endl;
    std::exit(1);
  }
  huadb::lsn_t next_lsn;
  file >> next_lsn;
  file.close();
  file.clear();

  if (!fs::is_regular_file(log_name)) {
    std::cerr << "File not found: " << log_name << std::endl;
    std::exit(1);
  }
  file.open(log_name, std::fstream::binary);
  if (file.fail()) {
    std::cerr << "Failed to open file: " << log_name << std::endl;
    std::exit(1);
  }
  char buffer[huadb::LOG_SEGMENT_SIZE];
  huadb::lsn_t lsn = huadb::FIRST_LSN;
  while (!file.eof()) {
    file.read(buffer, huadb::LOG_SEGMENT_SIZE);
    if (file.gcount() == 0) {
      break;
    }
    if (file.gcount() != huadb::LOG_SEGMENT_SIZE) {
      std::cerr << "Incorrect log segment size" << std::endl;
      std::exit(1);
    }
    while (lsn < next_lsn) {
      auto log = huadb::LogRecord::DeserializeFrom(lsn, buffer + lsn);
      std::cout << log->ToString() << std::endl;
      lsn += log->GetSize();
    }
    if (lsn >= next_lsn) {
      break;
    }
  }
}

int main(int argc, char *argv[]) {
  argparse::ArgumentParser program("huadb-parser");
  program.add_argument("-c", "--control").flag();
  program.add_argument("-d", "--data").flag();
  program.add_argument("-l", "--log").flag();
  program.add_argument("path").nargs(1).default_value(std::string(huadb::BASE_PATH));
  try {
    program.parse_args(argc, argv);
  } catch (const std::exception &err) {
    std::cerr << err.what() << std::endl;
    std::cerr << program;
    std::exit(1);
  }
  if (program.get<bool>("-c") + program.get<bool>("-d") + program.get<bool>("-l") != 1) {
    std::cerr << "Exactly one of -c, -d, -l must be specified" << std::endl;
    std::exit(1);
  }

  ParseMode mode;
  if (program.get<bool>("-c")) {
    mode = ParseMode::CONTROL;
  } else if (program.get<bool>("-d")) {
    mode = ParseMode::DATA;
  } else {
    mode = ParseMode::LOG;
  }

  fs::path path = program.get<std::string>("path");

  if (mode == ParseMode::CONTROL) {
    parse_control(path);
  } else if (mode == ParseMode::DATA) {
    parse_data(path);
  } else {
    parse_log(path);
  }

  return 0;
}
