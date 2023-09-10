if(NOT EMSCRIPTEN)
  add_executable(sqllogictest sqllogictest.cpp sqllogicparser.cpp)
  target_link_libraries(sqllogictest huadb)
endif()
