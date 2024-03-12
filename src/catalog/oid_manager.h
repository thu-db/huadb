#pragma once

#include <atomic>
#include <string>
#include <unordered_map>

#include "common/types.h"

namespace huadb {

enum class OidType { DATABASE, TABLE, INDEX };

// oid管理器，负责处理oid分配和查找功能
class OidManager {
 public:
  explicit OidManager(oid_t next_oid);

  // 添加新的条目，并分配新的oid
  oid_t CreateEntry(OidType type, const std::string &name);
  // 添加新的条目，使用指定oid
  void SetEntryOid(OidType type, const std::string &name, oid_t oid);
  // 删除指定条目
  oid_t DropEntry(OidType type, const std::string &name);

  // 根据oid查找条目名称
  std::string GetEntryName(oid_t oid) const;
  // 根据类型和名称查找oid
  oid_t GetEntryOid(OidType type, const std::string &name) const;
  // 判断条目是否存在
  bool EntryExists(OidType type, const std::string &name) const;
  bool OidExists(oid_t oid) const;

  oid_t GetNextOid() const;

 private:
  static std::string GetEntryPrefix(OidType type);
  // oid相关信息
  std::atomic<oid_t> next_oid_;
  std::unordered_map<std::string, oid_t> name2oid_;
  std::unordered_map<oid_t, std::string> oid2name_;
};

}  // namespace huadb
