#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "catalog/column_list.h"
#include "catalog/oid_manager.h"
#include "common/constants.h"

namespace huadb {

class BufferPool;
class LogManager;
class Table;
class Index;

class SimpleCatalog {
 public:
  SimpleCatalog(BufferPool &buffer_pool, LogManager &log_manager, oid_t next_oid = PRESERVED_OID);
  // 创建系统表，仅初始化系统使用
  void CreateSystemTables();
  // 加载系统表，系统已经初始化过时使用
  void LoadSystemTables();

  // 创建数据库，可指定oid
  void CreateDatabase(const std::string &database_name, bool exists_ok, oid_t db_oid = INVALID_OID);
  // 删除数据库
  void DropDatabase(const std::string &database_name, bool missing_ok);
  // 获取所有数据库名
  std::vector<std::string> GetDatabaseNames() const;
  // 切换当前数据库
  void ChangeDatabase(const std::string &database_name);
  void ChangeDatabase(oid_t db_oid);
  // 获取表所在的数据库的oid
  oid_t GetDatabaseOid(oid_t table_oid) const;

  // 创建表
  void CreateTable(const std::string &table_name, const ColumnList &column_list, oid_t oid = INVALID_OID,
                   oid_t db_oid = INVALID_OID, bool new_table = true);
  // 删除表
  void DropTable(const std::string &table_name);
  // 创建索引
  void CreateIndex(const std::string &index_name, const std::string &table_name);
  // 删除索引
  void DropIndex(const std::string &index_name);
  // 获取当前数据库下所有表名
  std::vector<std::string> GetTableNames() const;
  // 获取表
  std::shared_ptr<Table> GetTable(oid_t oid) const;
  // 获取表oid
  oid_t GetTableOid(const std::string &table_name) const;
  // 获取表的schema信息
  const ColumnList &GetTableColumnList(oid_t oid) const;
  const ColumnList &GetTableColumnList(const std::string &table_name) const;
  // 表是否存在
  bool TableExists(oid_t oid) const;
  // 获取下一个 oid
  oid_t GetNextOid() const;
  // 获取统计信息
  uint32_t GetCardinality(const std::string &table_name) const;
  uint32_t GetDistinct(const std::string &table_name, const std::string &column_name) const;
  // 设置统计信息
  void SetCardinality(const std::string &table_name, uint32_t cardinality);
  void SetDistinct(const std::string &table_name, const std::string &column_name, uint32_t distinct);

 private:
  BufferPool &buffer_pool_;
  LogManager &log_manager_;

  OidManager oid_manager_;
  // 对象映射表
  std::unordered_map<std::string, oid_t> name2oid_;
  std::unordered_map<oid_t, std::shared_ptr<Table>> oid2table_;
  std::unordered_map<oid_t, std::shared_ptr<Index>> oid2index_;
  std::unordered_map<std::string, uint32_t> table2cardinality_;
  std::unordered_map<std::string, uint32_t> col2distinct_;

  oid_t current_database_oid_ = INVALID_OID;
};

}  // namespace huadb
