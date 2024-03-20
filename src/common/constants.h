#pragma once

#include "common/types.h"

// 通过 SIMPLE_CATALOG 宏来切换 Catalog 实现
#ifndef SIMPLE_CATALOG
#define SystemCatalog Catalog
#else
#define SimpleCatalog Catalog
#endif

namespace huadb {

static constexpr const char *RESET = "\033[0m";
static constexpr const char *RED = "\033[31m";
static constexpr const char *GREEN = "\033[32m";
static constexpr const char *BOLD = "\033[1m";

static constexpr const char *BASE_PATH = "huadb_data";
static constexpr const char *LOG_NAME = "log";
static constexpr const char *INIT_NAME = "init";
static constexpr const char *CONTROL_NAME = "control";
static constexpr const char *NEXT_LSN_NAME = "next_lsn";
static constexpr const char *MASTER_RECORD_NAME = "master_record";

static constexpr size_t LOG_SEGMENT_SIZE = (1 << 20);
static constexpr size_t DB_PAGE_SIZE = (1 << 8);
static constexpr size_t MAX_RECORD_SIZE = 230;
// 日志记录最长长度
static constexpr size_t MAX_LOG_SIZE = sizeof(enum_t) + sizeof(xid_t) + sizeof(lsn_t) + sizeof(oid_t) + sizeof(oid_t) +
                                       sizeof(pageid_t) + sizeof(slotid_t) + sizeof(db_size_t) + sizeof(db_size_t) +
                                       MAX_RECORD_SIZE + sizeof(lsn_t);
static constexpr size_t BUFFER_SIZE = 5;

static constexpr lsn_t FIRST_LSN = 0;
static constexpr lsn_t NULL_LSN = -1;

static constexpr xid_t NULL_XID = -1;
static constexpr cid_t NULL_CID = -1;
static constexpr xid_t FIRST_XID = 1;
static constexpr cid_t FIRST_CID = 1;
static constexpr xid_t DDL_XID = 0;
static constexpr cid_t DDL_CID = 0;

static constexpr pageid_t NULL_PAGE_ID = 0xFFFFFFFF;

// Catalog 相关
static constexpr oid_t INVALID_OID = -1;
static constexpr oid_t PRESERVED_OID = 10000;

static constexpr oid_t SYSTEM_DATABASE_OID = 1;
static constexpr oid_t TEMP_DATABASE_OID = 2;

static constexpr oid_t TABLE_META_OID = 501;
static constexpr oid_t DATABASE_META_OID = 502;
static constexpr oid_t STATISTIC_META_OID = 503;

static constexpr uint32_t INVALID_CARDINALITY = -1;
static constexpr uint32_t INVALID_DISTINCT = -1;

static constexpr const char *SYSTEM_DATABASE_NAME = "system";

static constexpr const char *TABLE_META_NAME = "huadb_table";
static constexpr const char *DATABASE_META_NAME = "huadb_database";
static constexpr const char *STATISTIC_META_NAME = "huadb_statistic";

static constexpr const char *DEFAULT_DATABASE_NAME = "huadb";

}  // namespace huadb
