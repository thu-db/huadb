#include "catalog/simple_catalog.h"

#include <cassert>
#include <string>

#include "common/constants.h"
#include "common/exceptions.h"
#include "table/table.h"

namespace huadb {

SimpleCatalog::SimpleCatalog(BufferPool &buffer_pool, LogManager &log_manager, oid_t next_oid)
    : buffer_pool_(buffer_pool), log_manager_(log_manager), oid_manager_(next_oid) {}

void SimpleCatalog::CreateSystemTables() {
  Disk::CreateDirectory(std::to_string(TEMP_DATABASE_OID));
  // 切换当前数据库oid
  current_database_oid_ = TEMP_DATABASE_OID;
}

void SimpleCatalog::LoadSystemTables() {
  // 切换当前数据库oid
  current_database_oid_ = TEMP_DATABASE_OID;
  // 读取所有的meta文件
  std::ifstream db_in(std::to_string(current_database_oid_) + "/tables");
  std::unordered_set<std::string> table_names;
  while (!db_in.eof()) {
    std::string table_name;
    db_in >> table_name;
    if (table_name.empty()) {
      break;
    }
    if (table_name[0] == '~') {
      table_names.erase(table_name.substr(1));
    } else {
      table_names.insert(table_name);
    }
  }
  for (const auto &table_name : table_names) {
    std::ifstream table_in(std::to_string(current_database_oid_) + "/" + table_name + ".meta");
    oid_t oid, db_oid;
    std::string name, desc;
    table_in >> oid >> db_oid >> name;
    while (!table_in.eof()) {
      std::string tmp;
      table_in >> tmp;
      desc += " ";
      desc += tmp;
    }
    ColumnList column_list;
    column_list.FromString(desc);
    CreateTable(name, column_list, oid, db_oid, false);
  }
}

void SimpleCatalog::CreateDatabase(const std::string &database_name, bool exists_ok, oid_t db_oid) {
  throw DbException("CreateDatabase not implemented in SimpleCatalog");
}

void SimpleCatalog::DropDatabase(const std::string &database_name, bool missing_ok) {
  throw DbException("DropDatabase not implemented in SimpleCatalog");
}

std::vector<std::string> SimpleCatalog::GetDatabaseNames() const { return {"tmp"}; }

oid_t SimpleCatalog::GetDatabaseOid(oid_t table_oid) const { return TEMP_DATABASE_OID; }

void SimpleCatalog::ChangeDatabase(oid_t db_oid) {
  throw DbException("ChangeDatabase not implemented in SimpleCatalog");
}

void SimpleCatalog::ChangeDatabase(const std::string &database_name) {
  throw DbException("ChangeDatabase not implemented in SimpleCatalog");
}

void SimpleCatalog::CreateTable(const std::string &table_name, const ColumnList &column_list, oid_t oid, oid_t db_oid,
                                bool new_table) {
  // Step1. 约束检测
  if (db_oid == INVALID_OID) {
    db_oid = current_database_oid_;
  }
  if (oid_manager_.EntryExists(OidType::TABLE, table_name)) {
    throw DbException("Table \"" + table_name + "\" already exists");
  }
  // Step2. OidManager添加对应项
  if (oid == INVALID_OID) {
    oid = oid_manager_.CreateEntry(OidType::TABLE, table_name);
  } else {
    oid_manager_.SetEntryOid(OidType::TABLE, table_name, oid);
  }

  // Step3. 创建新的表
  if (oid > PRESERVED_OID) {
    assert(db_oid != SYSTEM_DATABASE_OID);
  }
  if (new_table) {
    Disk::CreateFile(Disk::GetFilePath(db_oid, oid));
  }
  name2oid_[table_name] = oid;
  oid2table_[oid] = std::make_shared<Table>(buffer_pool_, log_manager_, oid, db_oid, column_list, new_table,
                                            Disk::EmptyFile(Disk::GetFilePath(db_oid, oid)));

  // 检查：非新表不需要添加到Meta中
  if (!new_table) {
    return;
  }

  // Step4. 写入到持久化文件table_name.meta
  std::ofstream out(std::to_string(current_database_oid_) + "/" + table_name + ".meta");
  out << oid << " " << current_database_oid_ << " " << table_name << " " << column_list.ToString();
  std::ofstream db_out(std::to_string(current_database_oid_) + "/tables", std::ios::app);
  db_out << table_name << " ";
}

void SimpleCatalog::DropTable(const std::string &table_name) {
  assert(current_database_oid_ != SYSTEM_DATABASE_OID);
  if (!oid_manager_.EntryExists(OidType::TABLE, table_name)) {
    throw DbException("Table \"" + table_name + "\" does not exist");
  }
  oid_t table_oid = oid_manager_.GetEntryOid(OidType::TABLE, table_name);
  // Step2. 实际删除表
  // 磁盘中删除对应项
  Disk::RemoveFile(Disk::GetFilePath(current_database_oid_, table_oid));
  name2oid_.erase(table_name);
  oid2table_.erase(table_oid);

  // Step3. OidManager删除对应项
  oid_manager_.DropEntry(OidType::TABLE, table_name);
  // Step4: 删除对应的meta文件
  Disk::RemoveFile(std::to_string(current_database_oid_) + "/" + table_name + ".meta");
  std::ofstream db_out(std::to_string(current_database_oid_) + "/tables", std::ios::app);
  db_out << "~" << table_name << " ";
}

void SimpleCatalog::CreateIndex(const std::string &index_name, const std::string &table_name) {
  throw DbException("ChangeIndex not implemented in SimpleCatalog");
}

void SimpleCatalog::DropIndex(const std::string &index_name) {
  throw DbException("DropIndex not implemented in SimpleCatalog");
}

std::vector<std::string> SimpleCatalog::GetTableNames() const {
  std::vector<std::string> table_names;
  for (const auto &[name, _] : name2oid_) {
    table_names.push_back(name);
  }
  return table_names;
}

std::shared_ptr<Table> SimpleCatalog::GetTable(oid_t oid) const {
  if (oid2table_.find(oid) == oid2table_.end()) {
    throw DbException("Table with oid " + std::to_string(oid) + " does not exist");
  }
  return oid2table_.at(oid);
}

oid_t SimpleCatalog::GetTableOid(const std::string &table_name) const {
  if (!oid_manager_.EntryExists(OidType::TABLE, table_name)) {
    throw DbException("Table \"" + table_name + "\" does not exist");
  }
  return oid_manager_.GetEntryOid(OidType::TABLE, table_name);
}

const ColumnList &SimpleCatalog::GetTableColumnList(oid_t oid) const { return GetTable(oid)->GetColumnList(); }

const ColumnList &SimpleCatalog::GetTableColumnList(const std::string &table_name) const {
  auto oid = GetTableOid(table_name);
  return GetTable(oid)->GetColumnList();
}

bool SimpleCatalog::TableExists(oid_t oid) const { return oid_manager_.OidExists(oid); }

oid_t SimpleCatalog::GetNextOid() const { return oid_manager_.GetNextOid(); }

uint32_t SimpleCatalog::GetCardinality(const std::string &table_name) const { return INVALID_CARDINALITY; }

uint32_t SimpleCatalog::GetDistinct(const std::string &table_name, const std::string &column_name) const {
  return INVALID_DISTINCT;
}

void SimpleCatalog::SetCardinality(const std::string &table_name, uint32_t cardinality) {}

void SimpleCatalog::SetDistinct(const std::string &table_name, const std::string &column_name, uint32_t distinct) {}

}  // namespace huadb
