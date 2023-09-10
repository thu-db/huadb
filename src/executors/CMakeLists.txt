add_library(
  executors
  OBJECT
  aggregate_executor.cpp
  delete_executor.cpp
  filter_executor.cpp
  hash_join_executor.cpp
  insert_executor.cpp
  limit_executor.cpp
  lock_rows_executor.cpp
  merge_join_executor.cpp
  nested_loop_join_executor.cpp
  orderby_executor.cpp
  projection_executor.cpp
  seqscan_executor.cpp
  update_executor.cpp
  values_executor.cpp
)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:executors>
  PARENT_SCOPE)
