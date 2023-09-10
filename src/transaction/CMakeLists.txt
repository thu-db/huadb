add_library(
  transaction
  OBJECT
  lock_manager.cpp
  transaction_manager.cpp
)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:table>
  PARENT_SCOPE)
