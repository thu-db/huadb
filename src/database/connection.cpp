#include "database/connection.h"

#include "database/database_engine.h"

namespace huadb {

Connection::Connection(DatabaseEngine &database) : database_(database) {}

void Connection::SendQuery(const std::string &sql, ResultWriter &writer) { database_.ExecuteSql(sql, writer, *this); }

}  // namespace huadb
