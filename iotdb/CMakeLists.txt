cmake_minimum_required(VERSION 3.5)
project (iotdb CXX)

# Custome CMake find instructions and macros
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")
include(cmake/macros.cmake)

# C++ Standard
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set Optimization Flags
set(CMAKE_CXX_FLAGS "-Wall")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-Wextra -O3")

# Print definition values.
message( STATUS "Clang Executable: ${CLANG_EXECUTABLE}" )
message( STATUS "Clang-Format Executable: ${CLANG_FORMAT_EXECUTABLE}" )
message( STATUS "Generated Code Directory: ${GENERATED_CODE_DIRECTORY}" )
message( STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}" )

# Add definitions as compile flag.
add_definitions( -D 'CLANG_EXECUTABLE="${CLANG_EXECUTABLE}"' )
add_definitions( -D 'CLANG_FORMAT_EXECUTABLE="${CLANG_FORMAT_EXECUTABLE}"' )
add_definitions( -D 'GENERATED_CODE_DIRECTORY="${GENERATED_CODE_DIRECTORY}/"' )
add_definitions( -D 'TEST_DATA_DIRECTORY="${TEST_DATA_DIRECTORY}/"' )


# Compiler should produce specific code for system architecture
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mtune=native -mavx")
endif()

# Threads
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
set(LIBRARIES ${LIBRARIES} Threads::Threads)

# LLVM
#find_package(LLVM 3.6.0 REQUIRED)

# Boost Libraries
find_package(Boost 1.47.0 REQUIRED system thread filesystem) # Only check if lib is available on system for generated code.
link_directories(${Boost_LIBRARY_DIRS})
include_directories(${Boost_INCLUDE_DIRS})
set(LIBRARIES ${LIBRARIES} ${Boost_LIBRARIES})

# Intel TBB
#find_package(TBB REQUIRED) # Only check if lib is available on system for generated code.
# link_directories(${TBB_INCLUDE_DIRS})
# include_directories(${TBB_INCLUDE_DIRS})
# set(LIBRARIES ${LIBRARIES} ${TBB_LIBRARIES})

# Use Performance Counters?
if(USE_PERFORMANCE_COUNTER)
    find_package(PAPI)
    set(LIBRARIES ${LIBRARIES} ${PAPI_LIBRARIES})
endif(USE_PERFORMANCE_COUNTER)

# Add Source Code
add_subdirectory(impl)


# Add Library
get_source_iotdb(iotdb_SOURCE_FILES)
#get_header_iotdb(iotdb_HEADER_FILES)
add_library(iotdb ${iotdb_SOURCE_FILES} ${iotdb_HEADER_FILES})
target_include_directories(iotdb PUBLIC "include")

add_executable(iot start.cpp)
target_include_directories(iot PUBLIC "include")
