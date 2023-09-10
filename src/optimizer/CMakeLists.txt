add_library(
  optimizer
  OBJECT
  optimizer.cpp
)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:optimizer>
  PARENT_SCOPE)
