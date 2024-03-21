#include "storage/disk.h"

#include <filesystem>
#include <iostream>

#include "common/constants.h"
#include "common/exceptions.h"
#include "common/string_util.h"

namespace huadb {

Disk::Disk() {
  if (!DirectoryExists(BASE_PATH)) {
    CreateDirectory(BASE_PATH);
  }
  ChangeDirectory(BASE_PATH);

  if (!FileExists(LOG_NAME)) {
    CreateFile(LOG_NAME);
    std::filesystem::resize_file(LOG_NAME, LOG_SEGMENT_SIZE);
    log_segments = 1;
  } else {
    auto log_file_size = std::filesystem::file_size(LOG_NAME);
    if (log_file_size / LOG_SEGMENT_SIZE == 0 || log_file_size % LOG_SEGMENT_SIZE != 0) {
      throw DbException("log file size is not a multiple of segment size");
    }
    log_segments = log_file_size / LOG_SEGMENT_SIZE;
  }
  log_fs_.open(LOG_NAME, std::fstream::in | std::fstream::out | std::fstream::binary);
  if (log_fs_.fail()) {
    throw DbException("fstream failed in Disk::Disk");
  }
}

Disk::~Disk() { ChangeDirectory(".."); }

bool Disk::DirectoryExists(const std::string &path) { return std::filesystem::is_directory(path); }

void Disk::ChangeDirectory(const std::string &path) {
  if (!DirectoryExists(path)) {
    throw DbException("Directory " + path + " does not exist");
  }
  std::filesystem::current_path(path);
}

void Disk::CreateDirectory(const std::string &path) { std::filesystem::create_directory(path); }

void Disk::RemoveDirectory(const std::string &path) { std::filesystem::remove_all(path); }

bool Disk::FileExists(const std::string &path) { return std::filesystem::is_regular_file(path); }

bool Disk::EmptyFile(const std::string &path) {
  if (!FileExists(path)) {
    throw DbException("file " + path + " does not exist");
  }
  return std::filesystem::is_empty(path);
}

void Disk::CreateFile(const std::string &path) { std::ofstream ofs(path); }

void Disk::RemoveFile(const std::string &path) { std::filesystem::remove(path); }

void Disk::OpenFile(const std::string &path) {
  hashmap_[path] = std::fstream(path, std::fstream::in | std::fstream::out | std::fstream::binary);
  if (!hashmap_[path]) {
    throw DbException("file " + path + " does not exist");
  }
}

void Disk::CloseFile(const std::string &path) { hashmap_.erase(path); }

void Disk::ReadPage(const std::string &path, pageid_t page_id, char *data) {
  if (GetOid(path).first != SYSTEM_DATABASE_OID) {
    access_count_++;
  }
  if (hashmap_.count(path) == 0) {
    OpenFile(path);
  }
  auto &fs = hashmap_[path];
  if (fs.fail()) {
    throw DbException("fstream failed in Disk::ReadPage");
  }
  fs.seekg(page_id * DB_PAGE_SIZE);
  fs.read(data, DB_PAGE_SIZE);
  if (fs.gcount() != DB_PAGE_SIZE) {
    throw DbException(path + " read page " + std::to_string(page_id) + " failed: read " + std::to_string(fs.gcount()) +
                      " bytes, expected " + std::to_string(DB_PAGE_SIZE) + " bytes");
  }
}

void Disk::WritePage(const std::string &path, pageid_t page_id, const char *data) {
  if (!FileExists(path)) {
    return;
  }
  if (hashmap_.count(path) == 0) {
    OpenFile(path);
  }
  if (GetOid(path).first != SYSTEM_DATABASE_OID) {
    access_count_++;
  }
  auto &fs = hashmap_[path];
  if (fs.fail()) {
    throw DbException("fstream failed in Disk::WritePage");
  }
  fs.seekp(page_id * DB_PAGE_SIZE);
  fs.write(data, DB_PAGE_SIZE);
  fs.flush();
}

void Disk::ReadLog(uint32_t offset, uint32_t count, char *data) {
  if (log_fs_.fail()) {
    throw DbException("fstream failed in Disk::ReadLog");
  }
  log_fs_.seekg(offset);
  log_fs_.read(data, count);
  if (log_fs_.gcount() != count) {
    throw DbException("read log failed (offset: " + std::to_string(offset) + ", count: " + std::to_string(count) +
                      ", read: " + std::to_string(log_fs_.gcount()) + ")");
  }
}

void Disk::WriteLog(uint32_t offset, uint32_t count, const char *data) {
  if (log_fs_.fail()) {
    throw DbException("fstream failed in Disk::WriteLog");
  }
  if (offset + count > log_segments * LOG_SEGMENT_SIZE) {
    log_segments++;
    std::filesystem::resize_file(LOG_NAME, log_segments * LOG_SEGMENT_SIZE);
  }
  log_fs_.seekp(offset);
  log_fs_.write(data, count);
  log_fs_.flush();
}

uint32_t Disk::GetAccessCount() const { return access_count_; }

std::string Disk::GetFilePath(oid_t db_oid, oid_t table_oid) {
  return std::to_string(db_oid) + "/" + std::to_string(table_oid);
}

std::pair<oid_t, oid_t> Disk::GetOid(const std::string &path) {
  auto oids = StringUtil::Split(path, '/');
  return {std::stoi(oids[0]), std::stoi(oids[1])};
}

}  // namespace huadb
