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

int main(int argc, char *argv[]) {
  argparse::ArgumentParser program("huadb-parser");
  program.add_argument("-c", "--control").flag();
  program.add_argument("-d", "--data").flag();
  program.add_argument("-l", "--log").flag();
  program.add_argument("filename").nargs(1);
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

  auto filename = program.get<std::string>("filename");
  if (!fs::is_regular_file(filename)) {
    std::cerr << "File not found: " << filename << std::endl;
    std::exit(1);
  }
  std::ifstream file(filename, std::fstream::binary);
  if (file.fail()) {
    std::cerr << "Failed to open file: " << filename << std::endl;
    std::exit(1);
  }

  if (mode == ParseMode::CONTROL) {
    huadb::xid_t xid;
    huadb::lsn_t lsn;
    huadb::oid_t oid;
    bool normal_shutdown;
    file >> xid >> lsn >> oid >> normal_shutdown;
    std::cout << "xid: " << xid << std::endl;
    std::cout << "lsn: " << lsn << std::endl;
    std::cout << "oid: " << oid << std::endl;
    std::cout << "normal_shutdown: " << normal_shutdown << std::endl;
  } else if (mode == ParseMode::DATA) {
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
  } else {
    std::string line;
    while (std::getline(file, line)) {
      if (line[0] == 'L') {
        std::cout << line << std::endl;
      }
    }
  }

  return 0;
}
