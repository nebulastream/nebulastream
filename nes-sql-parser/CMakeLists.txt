cmake_minimum_required(VERSION 3.29)
project(nes_frontend)

set(CMAKE_CXX_STANDARD "14")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)
include_directories(src/libraries/pg_query/include)
add_subdirectory(src)
add_subdirectory(nebulastream_tests)

# include_directories(src/bin)

add_executable(playground src/bin/playground.cpp)

target_link_libraries(playground nebula_parser)
