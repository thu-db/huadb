add_library(
  binder
  OBJECT
  binder.cpp
  node_tag_to_string.cpp
)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:binder>
  PARENT_SCOPE)
