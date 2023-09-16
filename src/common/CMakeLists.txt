add_library(
  common
  OBJECT
  bitmap.cpp
  string_util.cpp
  type_util.cpp
  value.cpp
)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:common>
  PARENT_SCOPE)
