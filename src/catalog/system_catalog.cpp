#include "catalog/system_catalog.h"

#include <cassert>

#include "catalog/system_schema.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/value.h"
#include "table/record.h"
#include "table/table.h"
#include "table/table_scan.h"

namespace huadb {

SystemCatalog::SystemCatalog(BufferPool &buffer_pool, LogManager &log_manager, oid_t next_oid)
    : buffer_pool_(buffer_pool), log_manager_(log_manager), oid_manager_(next_oid) {}

void SystemCatalog::CreateSystemTables() {
  // 创建 system 数据库
  Disk::CreateDirectory(std::to_string(SYSTEM_DATABASE_OID));
  // 创建系统表
  CreateTable(TABLE_META_NAME, table_meta_schema, TABLE_META_OID, SYSTEM_DATABASE_OID, true);
  CreateTable(DATABASE_META_NAME, database_meta_schema, DATABASE_META_OID, SYSTEM_DATABASE_OID, true);
  CreateTable(STATISTIC_META_NAME, statistic_schema, STATISTIC_META_OID, SYSTEM_DATABASE_OID, true);
  // 插入默认数据库
  CreateDatabase(SYSTEM_DATABASE_NAME, false, SYSTEM_DATABASE_OID);
  CreateDatabase(DEFAULT_DATABASE_NAME, false);
  // 切换当前数据库
  ChangeDatabase(DEFAULT_DATABASE_NAME);
}

void SystemCatalog::LoadSystemTables() {
  // 加载默认的系统表
  // 所有系统表需要预先创建，否则恢复系统表信息时将产生段错误
  CreateTable(TABLE_META_NAME, table_meta_schema, TABLE_META_OID, SYSTEM_DATABASE_OID, false);
  CreateTable(DATABASE_META_NAME, database_meta_schema, DATABASE_META_OID, SYSTEM_DATABASE_OID, false);
  CreateTable(STATISTIC_META_NAME, statistic_schema, STATISTIC_META_OID, SYSTEM_DATABASE_OID, false);
  // 加载数据库信息
  LoadDatabaseMeta();

  ChangeDatabase(DEFAULT_DATABASE_NAME);
}

void SystemCatalog::CreateDatabase(const std::string &database_name, bool exists_ok, oid_t db_oid) {
  // Step 1. 约束检测
  if (!exists_ok && DatabaseExists(database_name)) {
    throw DbException("Database \"" + database_name + "\" already exists");
  }
  // Step 2. OidManager 添加对应项
  if (db_oid == INVALID_OID) {
    db_oid = oid_manager_.CreateEntry(OidType::DATABASE, database_name);
  } else {
    oid_manager_.SetEntryOid(OidType::DATABASE, database_name, db_oid);
  }
  // Step 3. DatabaseMeta 中添加对应记录
  std::vector<Value> values;
  values.emplace_back(db_oid);
  values.emplace_back(database_name);
  GetTable(DATABASE_META_OID)->InsertRecord(std::make_shared<Record>(std::move(values)), DDL_XID, DDL_CID, false);
  // Step 4. 实际创建数据库的文件夹
  if (!Disk::DirectoryExists(std::to_string(db_oid))) {
    Disk::CreateDirectory(std::to_string(db_oid));
  }
}

void SystemCatalog::DropDatabase(const std::string &database_name, bool missing_ok) {
  // Step 1. 判断数据库是否存在
  if (!DatabaseExists(database_name)) {
    if (missing_ok) {
      return;
    } else {
      throw DbException("Database \"" + database_name + "\" does not exist");
    }
  }
  // Step 2. 获取删除数据库的 db_oid
  oid_t db_oid = oid_manager_.GetEntryOid(OidType::DATABASE, database_name);
  if (db_oid == current_database_oid_) {
    throw DbException("Cannot drop the currently open database");
  }

  // Step 3. TableMeta 中删除包含表
  auto table_meta = GetTable(TABLE_META_OID);
  auto scan = std::make_shared<TableScan>(buffer_pool_, table_meta, Rid{table_meta->GetFirstPageId(), 0});
  auto db_oid_idx = table_meta_schema.GetColumnIndex("db_oid");
  auto table_name_idx = table_meta_schema.GetColumnIndex("table_name");
  while (auto record = scan->GetNextRecord()) {
    if (record->GetValue(db_oid_idx).GetValue<oid_t>() == db_oid) {
      table_meta->DeleteRecord(record->GetRid(), DDL_XID, false);
      auto table_name = record->GetValue(table_name_idx).GetValue<std::string>();
    }
  }

  // Step 4. DatabaseMeta 中删除对应项
  bool deleted = false;
  auto db_meta = GetTable(DATABASE_META_OID);
  scan = std::make_shared<TableScan>(buffer_pool_, db_meta, Rid{db_meta->GetFirstPageId(), 0});
  db_oid_idx = database_meta_schema.GetColumnIndex("db_oid");
  while (auto record = scan->GetNextRecord()) {
    if (record->GetValue(db_oid_idx).GetValue<oid_t>() == db_oid) {
      db_meta->DeleteRecord(record->GetRid(), DDL_XID, false);
      deleted = true;
      break;
    }
  }
  if (!deleted) {
    throw DbException("Database \"" + database_name + "\" does not exist in Database Meta");
  }
  // Step 5. OidManager 删除对应项
  oid_manager_.DropEntry(OidType::DATABASE, database_name);
  // Step 6. 实际删除数据库的文件夹
  if (Disk::DirectoryExists(std::to_string(db_oid))) {
    Disk::RemoveDirectory(std::to_string(db_oid));
  }
}

std::vector<std::string> SystemCatalog::GetDatabaseNames() const {
  std::vector<std::string> db_names;
  assert(oid2table_.find(DATABASE_META_OID) != oid2table_.end());
  auto db_meta = GetTable(DATABASE_META_OID);
  auto scan = std::make_shared<TableScan>(buffer_pool_, db_meta, Rid{db_meta->GetFirstPageId(), 0});
  auto db_name_idx = database_meta_schema.GetColumnIndex("db_name");
  while (auto record = scan->GetNextRecord()) {
    auto db_name = record->GetValue(db_name_idx).GetValue<std::string>();
    db_names.push_back(db_name);
  }
  return db_names;
}

void SystemCatalog::ChangeDatabase(oid_t db_oid) {
  std::string db_name = oid_manager_.GetEntryName(db_oid);
  ChangeDatabase(db_name);
}

void SystemCatalog::ChangeDatabase(const std::string &database_name) {
  if (!DatabaseExists(database_name)) {
    throw DbException("Database \"" + database_name + "\" does not exist");
  }
  oid_t db_oid = oid_manager_.GetEntryOid(OidType::DATABASE, database_name);
  if (db_oid == current_database_oid_) {
    return;
  }
  // 判断是否需要先退出
  if (current_database_oid_ != INVALID_OID) {
    ExitDatabase();
  }
  // 特判：系统表默认常驻内存，跳过读取
  if (db_oid == SYSTEM_DATABASE_OID) {
    current_database_oid_ = db_oid;
    return;
  }
  // 设定当前数据库 id
  current_database_oid_ = db_oid;
  // 加载切换数据库的所有表
  LoadTableMeta();
  LoadStatistics();
}

oid_t SystemCatalog::GetDatabaseOid(oid_t table_oid) const {
  if (oid2table_.find(table_oid) != oid2table_.end()) {
    return current_database_oid_;
  }
  auto table_meta = GetTable(TABLE_META_OID);
  auto scan = std::make_shared<TableScan>(buffer_pool_, table_meta, Rid{table_meta->GetFirstPageId(), 0});
  auto table_oid_idx = table_meta_schema.GetColumnIndex("table_oid");
  auto db_oid_idx = table_meta_schema.GetColumnIndex("db_oid");
  while (auto record = scan->GetNextRecord()) {
    if (record->GetValue(table_oid_idx).GetValue<oid_t>() == table_oid) {
      return record->GetValue(db_oid_idx).GetValue<oid_t>();
    }
  }
  throw DbException("Table with oid " + std::to_string(table_oid) + " does not exist");
}

void SystemCatalog::CreateTable(const std::string &table_name, const ColumnList &column_list, oid_t oid, oid_t db_oid,
                                bool new_table) {
  // Step 1. 约束检测
  if (db_oid == INVALID_OID) {
    CheckUsingDatabase();
    db_oid = current_database_oid_;
  }
  if (oid_manager_.EntryExists(OidType::TABLE, table_name)) {
    throw DbException("Table \"" + table_name + "\" already exists");
  }
  // Step 2. OidManager 添加对应项
  if (oid == INVALID_OID) {
    oid = oid_manager_.CreateEntry(OidType::TABLE, table_name);
  } else {
    oid_manager_.SetEntryOid(OidType::TABLE, table_name, oid);
  }

  // Step 3. 创建新的表
  if (oid > PRESERVED_OID && db_oid == SYSTEM_DATABASE_OID) {
    throw DbException("Cannot create table in system database");
  }
  if (new_table) {
    Disk::CreateFile(Disk::GetFilePath(db_oid, oid));
  }
  oid2table_[oid] = std::make_shared<Table>(buffer_pool_, log_manager_, oid, db_oid, column_list, new_table,
                                            Disk::EmptyFile(Disk::GetFilePath(db_oid, oid)));

  // 检查：非新表不需要添加到 Meta 中
  if (!new_table) {
    return;
  }

  // Step 4. TableMeta 中添加对应记录
  assert(db_oid != INVALID_OID);
  std::vector<Value> values;
  values.emplace_back(oid);
  values.emplace_back(db_oid);
  values.emplace_back(table_name);
  values.emplace_back(column_list.ToString());
  values.emplace_back(INVALID_CARDINALITY);
  GetTable(TABLE_META_OID)->InsertRecord(std::make_shared<Record>(std::move(values)), DDL_XID, DDL_CID, false);
}

void SystemCatalog::DropTable(const std::string &table_name) {
  // Step 1. 约束检测
  CheckUsingDatabase();
  assert(current_database_oid_ != SYSTEM_DATABASE_OID);
  if (!oid_manager_.EntryExists(OidType::TABLE, table_name)) {
    throw DbException("Table \"" + table_name + "\" does not exist");
  }
  oid_t table_oid = oid_manager_.GetEntryOid(OidType::TABLE, table_name);
  // Step 2. 实际删除表
  // 磁盘中删除对应项
  Disk::RemoveFile(Disk::GetFilePath(current_database_oid_, table_oid));
  oid2table_.erase(table_oid);

  // Step 3. OidManager 删除对应项
  oid_manager_.DropEntry(OidType::TABLE, table_name);
  // Step 4: TableMeta 删除对应条目
  bool deleted = false;
  auto table_meta = GetTable(TABLE_META_OID);
  auto scan = std::make_shared<TableScan>(buffer_pool_, table_meta, Rid{table_meta->GetFirstPageId(), 0});
  auto table_oid_idx = table_meta_schema.GetColumnIndex("table_oid");
  while (auto record = scan->GetNextRecord()) {
    if (record->GetValue(table_oid_idx).GetValue<oid_t>() == table_oid) {
      table_meta->DeleteRecord(record->GetRid(), DDL_XID, false);
      deleted = true;
      break;
    }
  }
  if (!deleted) {
    throw DbException("Table \"" + table_name + "\" does not exist in table_meta");
  }
}

void SystemCatalog::CreateIndex(const std::string &index_name, const std::string &table_name) {}

void SystemCatalog::DropIndex(const std::string &index_name) {}

std::vector<std::string> SystemCatalog::GetTableNames() const {
  if (current_database_oid_ == INVALID_OID) {
    throw DbException("Invalid database oid in GetDatabaseTableNames");
  }
  std::vector<std::string> table_names{};
  // 获取 db_oid 相同元素的 table_oid
  auto table_meta = GetTable(TABLE_META_OID);
  auto scan = std::make_shared<TableScan>(buffer_pool_, table_meta, Rid{table_meta->GetFirstPageId(), 0});
  auto db_oid_idx = table_meta_schema.GetColumnIndex("db_oid");
  auto table_name_idx = table_meta_schema.GetColumnIndex("table_name");
  while (auto record = scan->GetNextRecord()) {
    if (record->GetValue(db_oid_idx).GetValue<oid_t>() == current_database_oid_) {
      table_names.push_back(record->GetValue(table_name_idx).GetValue<std::string>());
    }
  }
  return table_names;
}

std::shared_ptr<Table> SystemCatalog::GetTable(oid_t oid) const {
  if (oid2table_.find(oid) == oid2table_.end()) {
    throw DbException("Table with oid " + std::to_string(oid) + " does not exist");
  }
  return oid2table_.at(oid);
}

oid_t SystemCatalog::GetTableOid(const std::string &table_name) const {
  if (!oid_manager_.EntryExists(OidType::TABLE, table_name)) {
    throw DbException("Table \"" + table_name + "\" does not exist");
  }
  return oid_manager_.GetEntryOid(OidType::TABLE, table_name);
}

const ColumnList &SystemCatalog::GetTableColumnList(oid_t oid) const { return GetTable(oid)->GetColumnList(); }

const ColumnList &SystemCatalog::GetTableColumnList(const std::string &table_name) const {
  auto oid = GetTableOid(table_name);
  return GetTable(oid)->GetColumnList();
}

bool SystemCatalog::TableExists(oid_t oid) const { return oid_manager_.OidExists(oid); }

oid_t SystemCatalog::GetNextOid() const { return oid_manager_.GetNextOid(); }

uint32_t SystemCatalog::GetCardinality(const std::string &table_name) const {
  if (table2cardinality_.find(table_name) == table2cardinality_.end()) {
    return INVALID_CARDINALITY;
  }
  return table2cardinality_.at(table_name);
}

uint32_t SystemCatalog::GetDistinct(const std::string &table_name, const std::string &column_name) const {
  if (col2distinct_.find(table_name + "." + column_name) == col2distinct_.end()) {
    return INVALID_DISTINCT;
  }
  return col2distinct_.at(table_name + "." + column_name);
}

void SystemCatalog::SetCardinality(const std::string &table_name, uint32_t cardinality) {
  auto table_meta = GetTable(TABLE_META_OID);
  auto scan = std::make_shared<TableScan>(buffer_pool_, table_meta, Rid{table_meta->GetFirstPageId(), 0});
  auto db_oid_idx = table_meta_schema.GetColumnIndex("db_oid");
  auto table_name_idx = table_meta_schema.GetColumnIndex("table_name");
  auto cardinality_idx = table_meta_schema.GetColumnIndex("cardinality");
  bool found = false;
  while (auto record = scan->GetNextRecord()) {
    if (record->GetValue(db_oid_idx).GetValue<oid_t>() == current_database_oid_ &&
        record->GetValue(table_name_idx).GetValue<std::string>() == table_name) {
      if (found) {
        throw DbException(table_name + " has more than one record in table_meta");
      }
      record->SetValue(cardinality_idx, Value(cardinality));
      table_meta->UpdateRecordInPlace(*record);
      table2cardinality_[table_name] = cardinality;
      found = true;
    }
  }
  if (!found) {
    throw DbException("Table\"" + table_name + "\" does not exist in table_meta");
  }
}

void SystemCatalog::SetDistinct(const std::string &table_name, const std::string &column_name, uint32_t distinct) {
  auto statistic = GetTable(STATISTIC_META_OID);
  auto scan = std::make_shared<TableScan>(buffer_pool_, statistic, Rid{statistic->GetFirstPageId(), 0});
  auto table_name_idx = statistic_schema.GetColumnIndex("table_name");
  auto db_oid_idx = statistic_schema.GetColumnIndex("db_oid");
  auto column_name_idx = statistic_schema.GetColumnIndex("column_name");
  auto n_distinct_idx = statistic_schema.GetColumnIndex("n_distinct");
  bool found = false;
  while (auto record = scan->GetNextRecord()) {
    if (record->GetValue(db_oid_idx).GetValue<oid_t>() == current_database_oid_ &&
        record->GetValue(table_name_idx).GetValue<std::string>() == table_name &&
        record->GetValue(column_name_idx).GetValue<std::string>() == column_name) {
      if (found) {
        throw DbException(table_name + "." + column_name + " has more than one record in statistic_meta");
      }
      record->SetValue(n_distinct_idx, Value(distinct));
      statistic->UpdateRecordInPlace(*record);
      col2distinct_[table_name + "." + column_name] = distinct;
      found = true;
    }
  }
  if (!found) {
    std::vector<Value> values;
    values.emplace_back(table_name);
    values.emplace_back(current_database_oid_);
    values.emplace_back(column_name);
    values.emplace_back(distinct);
    statistic->InsertRecord(std::make_shared<Record>(std::move(values)), DDL_XID, DDL_CID, false);
    col2distinct_[table_name + "." + column_name] = distinct;
  }
}

void SystemCatalog::ExitDatabase() {
  // 约束检测
  assert(current_database_oid_ != INVALID_OID);
  // 特判：系统表常驻内存，默认不清除系统表
  if (current_database_oid_ == SYSTEM_DATABASE_OID) {
    return;
  }
  buffer_pool_.Flush(true);
  // 直接利用 OidManager 信息进行删除
  std::vector<oid_t> deleted_oids{};
  for (const auto &[oid, _] : oid2table_) {
    if (oid <= PRESERVED_OID) {
      continue;
    }
    deleted_oids.push_back(oid);
  }
  for (const auto &oid : deleted_oids) {
    std::string table_name = oid_manager_.GetEntryName(oid);
    oid_manager_.DropEntry(OidType::TABLE, table_name);
    oid2table_.erase(oid);
  }
  // 设定数据库 id 为无效值
  current_database_oid_ = INVALID_OID;
}

void SystemCatalog::CheckUsingDatabase() const {
  if (current_database_oid_ == INVALID_OID) {
    throw DbException("Not using databases");
  }
}

bool SystemCatalog::DatabaseExists(const std::string &database_name) const {
  return oid_manager_.EntryExists(OidType::DATABASE, database_name);
}

void SystemCatalog::DropTable(oid_t oid) { DropTable(oid_manager_.GetEntryName(oid)); }

void SystemCatalog::LoadDatabaseMeta() {
  assert(oid2table_.find(DATABASE_META_OID) != oid2table_.end());
  auto db_meta = GetTable(DATABASE_META_OID);
  auto scan = std::make_shared<TableScan>(buffer_pool_, db_meta, Rid{db_meta->GetFirstPageId(), 0});
  auto db_oid_idx = database_meta_schema.GetColumnIndex("db_oid");
  auto db_name_idx = database_meta_schema.GetColumnIndex("db_name");
  while (auto record = scan->GetNextRecord()) {
    auto db_oid = record->GetValue(db_oid_idx).GetValue<oid_t>();
    auto db_name = record->GetValue(db_name_idx).GetValue<std::string>();
    oid_manager_.SetEntryOid(OidType::DATABASE, db_name, db_oid);
  }
}

void SystemCatalog::LoadTableMeta() {
  auto table_meta = GetTable(TABLE_META_OID);
  auto scan = std::make_shared<TableScan>(buffer_pool_, table_meta, Rid{table_meta->GetFirstPageId(), 0});
  auto table_oid_idx = table_meta_schema.GetColumnIndex("table_oid");
  auto db_oid_idx = table_meta_schema.GetColumnIndex("db_oid");
  auto table_name_idx = table_meta_schema.GetColumnIndex("table_name");
  auto schema_idx = table_meta_schema.GetColumnIndex("schema");
  auto cardinality_idx = table_meta_schema.GetColumnIndex("cardinality");
  while (auto record = scan->GetNextRecord()) {
    if (record->GetValue(db_oid_idx).GetValue<oid_t>() == current_database_oid_) {
      // 读取数据表信息
      auto oid = record->GetValue(table_oid_idx).GetValue<oid_t>();
      auto table_name = record->GetValue(table_name_idx).GetValue<std::string>();
      auto column_list_string = record->GetValue(schema_idx).GetValue<std::string>();
      auto cardinality = record->GetValue(cardinality_idx).GetValue<uint32_t>();
      ColumnList column_list;
      column_list.FromString(column_list_string);

      // 添加数据表
      oid_manager_.SetEntryOid(OidType::TABLE, table_name, oid);
      oid2table_[oid] = std::make_shared<Table>(buffer_pool_, log_manager_, oid, current_database_oid_, column_list,
                                                false, Disk::EmptyFile(Disk::GetFilePath(GetDatabaseOid(oid), oid)));
      table2cardinality_[table_name] = cardinality;
    }
  }
}

void SystemCatalog::LoadStatistics() {
  auto statistic = GetTable(STATISTIC_META_OID);
  auto scan = std::make_shared<TableScan>(buffer_pool_, statistic, Rid{statistic->GetFirstPageId(), 0});
  auto table_name_idx = statistic_schema.GetColumnIndex("table_name");
  auto db_oid_idx = statistic_schema.GetColumnIndex("db_oid");
  auto column_name_idx = statistic_schema.GetColumnIndex("column_name");
  auto n_distinct_idx = statistic_schema.GetColumnIndex("n_distinct");
  while (auto record = scan->GetNextRecord()) {
    if (record->GetValue(db_oid_idx).GetValue<oid_t>() == current_database_oid_) {
      auto table_name = record->GetValue(table_name_idx).GetValue<std::string>();
      auto column_name = record->GetValue(column_name_idx).GetValue<std::string>();
      auto n_distinct = record->GetValue(n_distinct_idx).GetValue<uint32_t>();
      col2distinct_[table_name + "." + column_name] = n_distinct;
    }
  }
}

}  // namespace huadb
