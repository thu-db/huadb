add_subdirectory(log_records)

add_library(
  log
  OBJECT
  log_manager.cpp
  log_record.cpp
)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:log>
  PARENT_SCOPE)
