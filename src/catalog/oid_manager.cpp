#include "catalog/oid_manager.h"

#include "common/exceptions.h"

namespace huadb {

OidManager::OidManager(oid_t next_oid) : next_oid_(next_oid) {}

oid_t OidManager::CreateEntry(OidType type, const std::string &name) {
  oid_t oid = next_oid_++;
  oid2name_[oid] = name;
  name2oid_[GetEntryPrefix(type) + name] = oid;
  return oid;
}

void OidManager::SetEntryOid(OidType type, const std::string &name, oid_t oid) {
  std::string entry_name = GetEntryPrefix(type) + name;
  oid2name_[oid] = name;
  name2oid_[entry_name] = oid;
}

oid_t OidManager::DropEntry(OidType type, const std::string &name) {
  oid_t oid = GetEntryOid(type, name);
  oid2name_.erase(oid);
  name2oid_.erase(GetEntryPrefix(type) + name);
  return oid;
}

std::string OidManager::GetEntryName(oid_t oid) const {
  if (oid2name_.find(oid) == oid2name_.end()) {
    throw DbException("Entry with oid " + std::to_string(oid) + " does not exist.");
  }
  return oid2name_.at(oid);
}

oid_t OidManager::GetEntryOid(OidType type, const std::string &name) const {
  std::string entry_name = GetEntryPrefix(type) + name;
  if (name2oid_.find(entry_name) == name2oid_.end()) {
    throw DbException(name + " not found in oid manager");
  }
  return name2oid_.at(entry_name);
}

bool OidManager::EntryExists(OidType type, const std::string &name) const {
  return name2oid_.find(GetEntryPrefix(type) + name) != name2oid_.end();
}

bool OidManager::OidExists(oid_t oid) const { return oid2name_.find(oid) != oid2name_.end(); }

oid_t OidManager::GetNextOid() const { return next_oid_; }

std::string OidManager::GetEntryPrefix(OidType type) {
  switch (type) {
    case OidType::TABLE:
      return "TABLE.";
    case OidType::DATABASE:
      return "DATABASE.";
    default:
      throw DbException("Unsupported object in oid system");
  }
}

}  // namespace huadb
