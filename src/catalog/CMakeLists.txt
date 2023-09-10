if(${SIMPLE_CATALOG})
  add_definitions(-DSIMPLE_CATALOG)
endif()

add_library(
  catalog
  OBJECT
  column_definition.cpp
  column_list.cpp
  oid_manager.cpp
  simple_catalog.cpp
  system_catalog.cpp
)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:catalog>
  PARENT_SCOPE)
