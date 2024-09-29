# don't export any symbols from the third party stuff
set(CMAKE_C_VISIBILITY_PRESET hidden)

add_subdirectory(libraries)
add_subdirectory(parser)
