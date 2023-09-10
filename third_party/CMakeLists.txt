set(FORT_ENABLE_TESTING OFF CACHE INTERNAL "")

add_compile_options(-w)

add_subdirectory(fmt)
add_subdirectory(libfort)
add_subdirectory(libpg_query)
add_subdirectory(linenoise)
