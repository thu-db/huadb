#include "sqllogicparser.h"

#include <fstream>

#include "common/exceptions.h"

bool SQLLogicParser::OpenFile(fs::path path) {
  if (!fs::is_regular_file(path)) {
    return false;
  }
  path_ = std::move(path);
  std::ifstream file(path_);
  if (file.bad() || file.fail()) {
    return false;
  }
  std::string line;
  while (std::getline(file, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    lines_.push_back(line);
  }
  return !file.bad();
}

std::vector<std::string> SQLLogicParser::Tokenize() {
  const auto &line = *line_iter_;

  const char delimiter = ' ';
  std::string::size_type pos;
  std::string::size_type last_pos = 0;
  std::string::size_type length = line.length();
  std::vector<std::string> tokens;

  while (last_pos < length + 1) {
    pos = line.find_first_of(delimiter, last_pos);
    if (pos == std::string::npos) {
      pos = length;
    }
    if (pos != last_pos) {
      tokens.emplace_back(line.substr(last_pos, pos - last_pos));
    }
    last_pos = line.find_first_not_of(delimiter, pos + 1);
  }

  return tokens;
}

void SQLLogicParser::Parse() {
  line_iter_ = lines_.cbegin();
  while (line_iter_ != lines_.cend()) {
    const auto &line = *line_iter_;
    if (line.empty() || line[0] == '#' || line == "\r") {
      line_iter_++;
      continue;
    }
    auto loc = Location{path_.parent_path().filename().string() + "/" + path_.filename().string(),
                        static_cast<size_t>(std::distance(lines_.cbegin(), line_iter_) + 1)};
    auto tokens = Tokenize();
    if (tokens.empty()) {
      line_iter_++;
      continue;
    }
    if (tokens[0] == "statement") {
      if (tokens.size() < 2) {
        throw huadb::DbException(loc.ToString() + ": Unexpected number of args in " + tokens[0]);
      }
      ResultType result_type;
      std::string connection_name;
      if (tokens[1] == "ok") {
        result_type = ResultType::SUCCESS;
      } else if (tokens[1] == "error") {
        result_type = ResultType::ERROR;
      } else {
        throw huadb::DbException(loc.ToString() + ": Unknown result type " + tokens[1]);
      }
      if (tokens.size() > 2) {
        connection_name = tokens[2];
      }
      if (line_iter_ != lines_.cend()) {
        line_iter_++;
      }
      std::string sql;
      while (line_iter_ != lines_.cend()) {
        const auto &line = *line_iter_;
        if (line.empty()) {
          break;
        }
        sql += line;
        sql += "\n";
        line_iter_++;
      }
      records.emplace_back(
          std::make_unique<StatementRecord>(loc, std::move(sql), result_type, std::move(connection_name)));
      if (line_iter_ == lines_.end()) {
        break;
      }
    } else if (tokens[0] == "query") {
      auto sort_mode = SortMode::NO_SORT;
      std::string connection_name;
      if (tokens.size() > 1) {
        if (tokens[1] == "rowsort") {
          sort_mode = SortMode::ROW_SORT;
        } else if (tokens[1] == "nosort") {
          sort_mode = SortMode::NO_SORT;
        } else {
          connection_name = tokens[1];
        }
      }
      if (tokens.size() > 2) {
        connection_name = tokens[2];
      }
      line_iter_++;
      if (line_iter_ == lines_.cend()) {
        throw huadb::DbException("Unexpected end of file");
      }

      std::string sql;
      bool has_result = false;
      while (line_iter_ != lines_.cend()) {
        const auto &line = *line_iter_;
        if (line == "----") {
          line_iter_++;
          has_result = true;
          break;
        }
        sql += line;
        sql += "\n";
        line_iter_++;
      }
      if (!has_result) {
        throw huadb::DbException(loc.ToString() + ": No result for query record");
      }

      std::string result;
      while (line_iter_ != lines_.end()) {
        const auto &line = *line_iter_;
        if (line.empty()) {
          break;
        }
        result += line;
        result += "\n";
        line_iter_++;
      }
      records.emplace_back(
          std::make_unique<QueryRecord>(loc, std::move(sql), sort_mode, std::move(connection_name), std::move(result)));
      if (line_iter_ == lines_.end()) {
        break;
      }
    } else {
      throw huadb::DbException(loc.ToString() + ": Unknown command " + tokens[0]);
    }
    line_iter_++;
  }
}
