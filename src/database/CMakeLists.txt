add_library(
  database
  OBJECT
  connection.cpp
  database_engine.cpp
)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:database>
  PARENT_SCOPE)
