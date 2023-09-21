#include "catalog/column_list.h"

namespace huadb {

// clang-format off
ColumnList table_meta_schema({ColumnDefinition("table_oid", Type::UINT),
                              ColumnDefinition("db_oid", Type::UINT),
                              ColumnDefinition("table_name", Type::VARCHAR, 32),
                              ColumnDefinition("schema", Type::VARCHAR, 1024),
                              ColumnDefinition("cardinality", Type::UINT)});
ColumnList database_meta_schema({ColumnDefinition("db_oid", Type::UINT),
                                 ColumnDefinition("db_name", Type::VARCHAR, 32)});
ColumnList statistic_schema({ColumnDefinition("table_name", Type::VARCHAR, 32),
                             ColumnDefinition("db_oid", Type::UINT),
                             ColumnDefinition("column_name", Type::VARCHAR, 32),
                             ColumnDefinition("n_distinct", Type::UINT)});
// clang-format on

}  // namespace huadb
