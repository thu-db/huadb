#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>

#include "linenoise.h"

namespace fs = std::filesystem;

int PlainShell(int client_socket) {
  std::string query;
  while (std::getline(std::cin, query)) {
    ssize_t bytes_sent = send(client_socket, query.c_str(), query.size(), 0);
    if (bytes_sent == -1) {
      std::cerr << "Failed to send query to the server" << std::endl;
      return 1;
    }
    char buffer[1 << 20];
    ssize_t bytes_read = recv(client_socket, buffer, sizeof(buffer), 0);
    if (bytes_read == -1) {
      std::cerr << "Failed to read from the server" << std::endl;
      return 1;
    } else {
      buffer[bytes_read] = '\0';
      std::cout << (buffer + 1);
    }
  }
  return 0;
}

int LinenoiseShell(int client_socket) {
  std::string history_file;
  auto *home_dir = getenv("HOME");
  if (home_dir != nullptr) {
    history_file = std::string(home_dir) + "/.huadb_history";
  } else {
    history_file = fs::absolute(".huadb_history");
  }
  linenoiseHistoryLoad(history_file.c_str());
  linenoiseHistorySetMaxLen(2048);
  linenoiseSetMultiLine(1);
  while (true) {
    std::string prompt = "huadb> ";
    std::string query;
    bool first_line = true;
    while (true) {
      auto line_prompt = first_line ? prompt : "...-> ";
      auto *query_c_str = linenoise(line_prompt.c_str());
      if (query_c_str == nullptr || std::string(query_c_str) == "\\q") {
        linenoiseHistorySave(history_file.c_str());
        return 0;
      }
      query += query_c_str;
      if (!query.empty() && (query.back() == ';' || query[0] == '\\')) {
        break;
      }
      first_line = false;
      query += " ";
      linenoiseFree(query_c_str);
    }
    linenoiseHistoryAdd(query.c_str());

    if (send(client_socket, query.c_str(), query.size(), 0) == -1) {
      std::cerr << "Failed to send query to the server" << std::endl;
      linenoiseHistorySave(history_file.c_str());
      return 1;
    }
    char buffer[1 << 20];
    ssize_t bytes_read = recv(client_socket, buffer, sizeof(buffer), 0);
    if (bytes_read == -1) {
      std::cerr << "Failed to read from the server" << std::endl;
      linenoiseHistorySave(history_file.c_str());
      return 1;
    } else {
      buffer[bytes_read] = '\0';
      std::cout << (buffer + 1);
    }
  }
  linenoiseHistorySave(history_file.c_str());
  return 0;
}

int main(int argc, char *argv[]) {
  signal(SIGPIPE, SIG_IGN);
  bool use_linenoise = true;
  if (argc > 1 && strcmp(argv[1], "-s") == 0) {
    use_linenoise = false;
  }
  int client_socket = socket(AF_UNIX, SOCK_STREAM, 0);
  if (client_socket == -1) {
    std::cerr << "Failed to create socket" << std::endl;
    return 1;
  }

  struct sockaddr_un server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sun_family = AF_UNIX;
  strncpy(server_addr.sun_path, "/tmp/huadb.sock", sizeof(server_addr.sun_path) - 1);

  if (connect(client_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
    std::cerr << "Failed to connect to the server" << std::endl;
    close(client_socket);
    return 1;
  }

  std::cout << "Connected to server" << std::endl;

  int rc = 0;
  if (use_linenoise) {
    rc = LinenoiseShell(client_socket);
  } else {
    rc = PlainShell(client_socket);
  }

  close(client_socket);

  return rc;
}
