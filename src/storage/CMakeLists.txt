add_library(
  storage
  OBJECT
  buffer_pool.cpp
  disk.cpp
  lru_buffer_strategy.cpp
  page.cpp
)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:storage>
  PARENT_SCOPE)
