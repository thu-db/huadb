#pragma once

#include <sstream>
#include <string>
#include <vector>

#include "fort.hpp"

namespace huadb {

class ResultWriter {
 public:
  virtual void WriteCell(const std::string &cell) = 0;
  virtual void WriteHeaderCell(const std::string &cell) = 0;
  virtual void BeginTable(bool simplified = false) = 0;
  virtual void EndTable() = 0;
  virtual void BeginHeader() = 0;
  virtual void EndHeader() = 0;
  virtual void BeginRow() = 0;
  virtual void EndRow() = 0;
  virtual void WriteRowCount(size_t count) = 0;
};

// Used in LinenoiseShell
class FortWriter : public ResultWriter {
 public:
  void WriteCell(const std::string &cell) override { table_ << cell; }
  void WriteHeaderCell(const std::string &cell) override { table_ << cell; }
  void BeginTable(bool simplified = false) override {
    if (simplified) {
      table_.set_border_style(FT_EMPTY_STYLE);
    }
  }
  void EndTable() override {
    tables_.push_back(table_.to_string());
    table_ = fort::utf8_table();
  }
  void BeginHeader() override { table_ << fort::header; }
  void EndHeader() override { table_ << fort::endr; }
  void BeginRow() override {}
  void EndRow() override { table_ << fort::endr; }
  void WriteRowCount(size_t count) override {
    if (count == 1) {
      tables_.push_back("(" + std::to_string(count) + " row)\n\n");
    } else {
      tables_.push_back("(" + std::to_string(count) + " rows)\n\n");
    }
  }

  std::vector<std::string> tables_;

 private:
  fort::utf8_table table_;
};

// Used in sqllogictest and SimpleShell
class SimpleWriter : public ResultWriter {
 public:
  explicit SimpleWriter(std::ostringstream &stream, bool disable_header = false, std::string separator = " ")
      : stream_(stream), disable_header_(disable_header), separator_(std::move(separator)) {}
  void WriteCell(const std::string &cell) override { stream_ << cell << separator_; }
  void WriteHeaderCell(const std::string &cell) override {
    if (!disable_header_) {
      stream_ << cell << separator_;
    }
  }
  void BeginTable(bool simplified = false) override {}
  void EndTable() override {}
  void BeginHeader() override {}
  void EndHeader() override {
    if (!disable_header_) {
      stream_ << std::endl;
    }
  }
  void BeginRow() override {}
  void EndRow() override { stream_ << std::endl; }
  void WriteRowCount(size_t count) override {}

 private:
  bool disable_header_;
  std::ostringstream &stream_;
  std::string separator_;
};

// Used in web shell
class HtmlWriter : public ResultWriter {
 public:
  explicit HtmlWriter(std::ostringstream &stream) : stream_(stream) {}
  void WriteCell(const std::string &cell) override { stream_ << "<td>" << cell << "</td>"; }
  void WriteHeaderCell(const std::string &cell) override { stream_ << "<td>" << cell << "</td>"; }
  void BeginTable(bool simplified = false) override { stream_ << "<table>"; }
  void EndTable() override { stream_ << "</table>"; }
  void BeginHeader() override { stream_ << "<thead><tr>"; }
  void EndHeader() override { stream_ << "</tr></thead>"; }
  void BeginRow() override { stream_ << "<tr>"; }
  void EndRow() override { stream_ << "</tr>"; }
  void WriteRowCount(size_t count) override {
    if (count == 1) {
      stream_ << "(" << count << " row)";
    } else {
      stream_ << "(" << count << " rows)";
    }
  }

 private:
  std::ostringstream &stream_;
};

}  // namespace huadb
