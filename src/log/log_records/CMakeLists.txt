add_library(
  log_records
  OBJECT
  begin_checkpoint_log.cpp
  begin_log.cpp
  commit_log.cpp
  delete_log.cpp
  end_checkpoint_log.cpp
  insert_log.cpp
  new_page_log.cpp
  rollback_log.cpp
)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:log>
  PARENT_SCOPE)
