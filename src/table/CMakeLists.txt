add_library(
  table
  OBJECT
  record_header.cpp
  record.cpp
  table_page.cpp
  table_scan.cpp
  table.cpp
)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:table>
  PARENT_SCOPE)
