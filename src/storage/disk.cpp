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
    log_fs_ = std::fstream(LOG_NAME, std::fstream::in | std::fstream::out | std::fstream::trunc);
  } else {
    log_fs_ = std::fstream(LOG_NAME, std::fstream::in | std::fstream::out);
  }
  std::filesystem::resize_file(LOG_NAME, LOG_SEGMENT_SIZE);
}

Disk::~Disk() {
  for (auto &entry : hashmap_) {
    entry.second.close();
  }
  ChangeDirectory("..");
}

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

void Disk::CreateFile(const std::string &path) {
  std::ofstream ofs(path);
  ofs.close();
}

void Disk::RemoveFile(const std::string &path) { std::filesystem::remove(path); }

void Disk::OpenFile(const std::string &path) {
  hashmap_[path] = std::fstream(path, std::fstream::in | std::fstream::out);
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
  fs.seekg(page_id * DB_PAGE_SIZE);
  fs.read(data, DB_PAGE_SIZE);
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
  fs.seekp(page_id * DB_PAGE_SIZE);
  fs.write(data, DB_PAGE_SIZE);
  fs.flush();
}

void Disk::ReadLog(uint32_t offset, uint32_t count, char *data) {
  log_fs_.seekg(offset);
  log_fs_.read(data, count);
}

void Disk::WriteLog(uint32_t offset, uint32_t count, const char *data) {
  log_fs_.seekp(offset);
  log_fs_.write(data, count);
  log_fs_.flush();
}

uint32_t Disk::GetAccessCount() { return access_count_; }

std::string Disk::GetFilePath(oid_t db_oid, oid_t table_oid) {
  return std::to_string(db_oid) + "/" + std::to_string(table_oid);
}

std::pair<oid_t, oid_t> Disk::GetOid(const std::string &path) {
  auto oids = StringUtil::Split(path, '/');
  return {std::stoi(oids[0]), std::stoi(oids[1])};
}

}  // namespace huadb
