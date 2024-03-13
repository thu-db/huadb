cmake_minimum_required(VERSION 3.15)

set(CMAKE_WARN_DEPRECATED OFF CACHE BOOL "" FORCE)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

project(HuaDB
  VERSION 2024
  DESCRIPTION "The educational database in THU database course"
  HOMEPAGE_URL https://github.com/thu-db/huadb
  LANGUAGES C CXX
)

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

set(THIRD_PARTY_INCLUDE_DIR
  ${PROJECT_SOURCE_DIR}/third_party/argparse/include
  ${PROJECT_SOURCE_DIR}/third_party/fmt/include
  ${PROJECT_SOURCE_DIR}/third_party/libfort/lib
  ${PROJECT_SOURCE_DIR}/third_party/libpg_query/include
  ${PROJECT_SOURCE_DIR}/third_party/linenoise
)

include_directories(${THIRD_PARTY_INCLUDE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/src)

function(disable_target_warnings NAME)
  target_compile_options(${NAME} PRIVATE "-w")
endfunction()

add_subdirectory(third_party)

add_subdirectory(src)
add_subdirectory(test)

find_package(Threads)
target_link_libraries(huadb ${CMAKE_THREAD_LIBS_INIT})
