add_subdirectory(binder)
add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(database)
add_subdirectory(executors)
add_subdirectory(log)
add_subdirectory(optimizer)
add_subdirectory(planner)
add_subdirectory(storage)
add_subdirectory(table)
add_subdirectory(transaction)

add_library(huadb STATIC ${ALL_OBJECT_FILES})

set(LIBS binder catalog common database executors log log_records optimizer planner storage table transaction)

set(THIRDPARTY_LIBS duckdb_pg_query fort fmt)

target_link_libraries(huadb ${LIBS} ${THIRDPARTY_LIBS})

add_subdirectory(bin)
