add_library(
  planner
  OBJECT
  planner.cpp
)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:planner>
  PARENT_SCOPE)
