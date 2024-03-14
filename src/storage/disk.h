#pragma once

#include <fstream>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/types.h"

namespace huadb {

class Disk {
 public:
  Disk();
  ~Disk();
  static bool DirectoryExists(const std::string &path);
  static void ChangeDirectory(const std::string &path);
  static void CreateDirectory(const std::string &path);
  static void RemoveDirectory(const std::string &path);

  static bool FileExists(const std::string &path);
  static bool EmptyFile(const std::string &path);

  static void CreateFile(const std::string &path);
  static void RemoveFile(const std::string &path);

  void OpenFile(const std::string &path);
  void CloseFile(const std::string &path);

  void ReadPage(const std::string &path, pageid_t page_id, char *data);
  void WritePage(const std::string &path, pageid_t page_id, const char *data);

  void ReadLog(uint32_t offset, uint32_t count, char *data);
  void WriteLog(uint32_t offset, uint32_t count, const char *data);

  uint32_t GetAccessCount() const;

  static std::string GetFilePath(oid_t db_oid, oid_t table_oid);

 private:
  static std::pair<oid_t, oid_t> GetOid(const std::string &path);
  std::unordered_map<std::string, std::fstream> hashmap_;  // 文件路径到 fstream 的映射表
  std::fstream log_fs_;

  uint32_t access_count_ = 0;  // 磁盘访问次数
  uint32_t log_segments = 0;   // 日志段数
};

}  // namespace huadb
