set(FORT_ENABLE_TESTING OFF CACHE INTERNAL "")

add_compile_options(-w)

add_subdirectory(argparse)
add_subdirectory(fmt)
add_subdirectory(libfort)
set(CMAKE_PROJECT_INCLUDE_BEFORE "${CMAKE_SOURCE_DIR}/cmake/enable_cmp0048.cmake")
add_subdirectory(libpg_query)
unset(CMAKE_PROJECT_INCLUDE_BEFORE)
add_subdirectory(linenoise)
