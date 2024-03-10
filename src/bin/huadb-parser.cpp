#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "argparse/argparse.hpp"

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
  std::ifstream file(filename);
  if (file.fail()) {
    std::cerr << "Failed to open file: " << filename << std::endl;
    std::exit(1);
  }

  return 0;
}
