cmake_minimum_required(VERSION 3.5.1)
project (tba CXX)

# C++ Standard
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set Optimization Flags
set(CMAKE_CXX_FLAGS "-Wall")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-Wextra -O3")

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
find_package(LLVM 3.6.0 REQUIRED)

# Boost Libraries
find_package(Boost 1.47.0 REQUIRED system thread filesystem) # Only check if lib is available on system for generated code.
link_directories(${Boost_LIBRARY_DIRS})
include_directories(${Boost_INCLUDE_DIRS})
set(LIBRARIES ${LIBRARIES} ${Boost_LIBRARIES})

# Intel TBB
find_package(TBB REQUIRED) # Only check if lib is available on system for generated code.
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

add_executable(tba start.cpp)

