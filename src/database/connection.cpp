#include "database/connection.h"

#include "database/database_engine.h"

namespace huadb {

Connection::Connection(DatabaseEngine &database) : database_(database) {}

void Connection::SendQuery(const std::string &sql, ResultWriter &writer) const {
  database_.ExecuteSql(sql, writer, *this);
}

bool Connection::InTransaction() const { return database_.InTransaction(*this); }

}  // namespace huadb
