#include <setjmp.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <functional>
#include <iostream>
#include <thread>

#include "common/constants.h"
#include "common/result_writer.h"
#include "database/connection.h"
#include "database/database_engine.h"

sigjmp_buf env;

void send_result(int client_socket, const std::string &result, const huadb::Connection &connection) {
  // auto current_db = connection.GetCurrentDatabase();
  // std::string in_transaction;
  // if (connection.InTransaction()) {
  //   in_transaction = "*";
  // }
  // auto prompt = current_db + "=" + in_transaction + "> ";
  send(client_socket, result.c_str(), result.size(), 0);
}

void sigint_handler(int signo) { siglongjmp(env, 1); }

void client_handler(int client_socket, huadb::DatabaseEngine &database) {
  std::cout << "Client connected" << std::endl;
  auto connection = std::make_unique<huadb::Connection>(database);

  char buffer[1 << 10];
  ssize_t bytes_read;
  while ((bytes_read = recv(client_socket, buffer, sizeof(buffer), 0)) > 0) {
    buffer[bytes_read] = '\0';
    std::cout << "Received: " << buffer << std::endl;
    try {
      auto writer = huadb::FortWriter();
      connection->SendQuery(buffer, writer);
      std::string result;
      for (const auto &table : writer.tables_) {
        result += table;
      }
      // Prevent empty result
      send_result(client_socket, "R" + result, *connection);
    } catch (std::exception &e) {
      std::ostringstream oss;
      oss << huadb::BOLD << huadb::RED << "Error: " << huadb::RESET << e.what() << "\n";
      send_result(client_socket, "R" + oss.str(), *connection);
    }
  }
  connection->Rollback();
  close(client_socket);
  std::cout << "Client disconnected" << std::endl;
}

int main() {
  signal(SIGINT, sigint_handler);

  auto database = std::make_unique<huadb::DatabaseEngine>();

  int server_socket = socket(AF_UNIX, SOCK_STREAM, 0);
  if (server_socket == -1) {
    std::cerr << "Failed to create socket" << std::endl;
    return 1;
  }

  unlink("/tmp/huadb.sock");

  struct sockaddr_un server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sun_family = AF_UNIX;
  strncpy(server_addr.sun_path, "/tmp/huadb.sock", sizeof(server_addr.sun_path) - 1);

  if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
    std::cerr << "Failed to bind socket" << std::endl;
    close(server_socket);
    return 1;
  }

  if (listen(server_socket, 5) == -1) {
    std::cerr << "Failed to listen on socket" << std::endl;
    close(server_socket);
    return 1;
  }

  std::cout << "Server started" << std::endl;

  struct sockaddr_un client_addr;
  socklen_t client_addr_len;
  while (true) {
    if (sigsetjmp(env, 1) == 1) {
      std::cout << "Shutting down server" << std::endl;
      break;
    }
    int client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &client_addr_len);
    if (client_socket == -1) {
      std::cerr << "Failed to accept connection" << std::endl;
      close(server_socket);
      return 1;
    }

    std::thread(client_handler, client_socket, std::ref(*database)).detach();
  }
  close(server_socket);
  unlink("/tmp/huadb.sock");
  return 0;
}
