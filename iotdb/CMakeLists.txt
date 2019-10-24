cmake_minimum_required(VERSION 3.5)
project (iotdb CXX)

if(POLICY CMP0004)
  cmake_policy(SET CMP0004 OLD)
endif()

# Custome CMake find instructions and macros
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")
include(cmake/macros.cmake)
include(${CMAKE_ROOT}/Modules/ExternalProject.cmake)

# C++ Standard
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set Optimization Flags
set(CMAKE_CXX_FLAGS "-Wall -g")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DNO_RACE_CHECK ")
set(CMAKE_CXX_FLAGS_RELEASE "-Wextra -O3")

# Compiler should produce specific code for system architecture
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mtune=native -mavx")
endif()

# LLVM
find_package(LLVM 3 REQUIRED)
find_package(clang REQUIRED)
message("-- LLVM version ${LLVM_VERSION_STRING}")

# ZMQ
find_package(ZMQ REQUIRED)
if(ZMQ_FOUND)
    message(STATUS "ZMQ found ${ZMQ_INCLUDE_DIRS}")
else(ZMQ_FOUND)
    message(ERROR "ZMQ not found")
endif(ZMQ_FOUND)
include_directories(${ZMQ_INCLUDE_DIRS})
set(LIBRARIES ${LIBRARIES} ${ZMQ_LIBRARIES})

# Log4cc
find_package(Log4cxx REQUIRED)
include_directories(${Log4cxx_INCLUDE_DIR})
set(LIBRARIES ${LIBRARIES} ${Log4cxx_LIBRARY})

if(LOG4CXX_FOUND)
    message(STATUS "LOG4CXX found")
else(LOG4CXX_FOUND)
    message(STATUS "LOG4CXX not found")
    message(STATUS "On Ubuntu, install the package by the following command!")
    message(STATUS "sudo apt-get install liblog4cxx10-dev")
endif(LOG4CXX_FOUND)


# TRAVIS-CI
if(TRAVIS_INCLUDE_DIR)
  include_directories(${TRAVIS_INCLUDE_DIR})
  message( STATUS "TRAVIS_INCLUDE_DIR:" ${TRAVIS_INCLUDE_DIR} )
endif()

# Boost Libraries
find_package(Boost 1.47.0 REQUIRED system thread program_options filesystem serialization chrono) # Only check if lib is available on system for generated code.
link_directories(${Boost_LIBRARY_DIRS})
include_directories(${Boost_INCLUDE_DIRS})
set(LIBRARIES ${LIBRARIES} ${Boost_LIBRARIES})

# Library containing dlopen and dlcose.
set(LIBRARIES ${LIBRARIES} ${CMAKE_DL_LIBS})

# C++ REST SDK
set(cpprestsdk_DIR ${CPPRESTSDK_DIR})
message("Found cpprestsdk in ${cpprestsdk_DIR}")
find_package(cpprestsdk REQUIRED)
# Library containing rest dependencies
set(LIBRARIES ${LIBRARIES} cpprestsdk::cpprest)

# Print all used include directories
message(STATUS "INCLUDE_DIRS:")
get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
foreach(dir ${dirs})
    message(STATUS "  - ${dir}")
endforeach()

message(STATUS " Libraries: ${LIBRARIES}")

# Definition of runtime variables ######################################################################################

# Set definition values.
set(CLANG_EXECUTABLE ${LLVM_ROOT_DIR}/bin/clang++)
set(CLANG_FORMAT_EXECUTABLE ${LLVM_ROOT_DIR}/bin/clang-format)
set(GENERATED_CODE_DIRECTORY ${PROJECT_BINARY_DIR}/generated-code)
set(PATH_TO_IOTDB_SOURCE_CODE ${PROJECT_SOURCE_DIR})

# Print definition values.
message( STATUS "Clang Executable: ${CLANG_EXECUTABLE}" )
message( STATUS "Clang-Format Executable: ${CLANG_FORMAT_EXECUTABLE}" )
message( STATUS "Generated Code Directory: ${GENERATED_CODE_DIRECTORY}" )

# Add definitions as compile flag.
add_definitions( -D 'CLANG_EXECUTABLE=\"${CLANG_EXECUTABLE}\"' )
add_definitions( -D 'CLANG_FORMAT_EXECUTABLE=\"${CLANG_FORMAT_EXECUTABLE}\"' )
add_definitions( -D 'GENERATED_CODE_DIRECTORY=\"${GENERATED_CODE_DIRECTORY}\"' )
add_definitions( -D 'PATH_TO_IOTDB_SOURCE_CODE=\"${PATH_TO_IOTDB_SOURCE_CODE}\"' )

add_definitions( -D 'QUERY_COMPILATION_CC=\"${QUERY_COMPILATION_CC}\"' )
add_definitions( -D 'CLANG_INCLUDE_DIRS=\"${CLANG_INCLUDE_DIRS}\"' )

# fix compile error with llvm (https://stackoverflow.com/a/18966387)
add_definitions( -D '__STDC_CONSTANT_MACROS')
add_definitions( -D '__STDC_LIMIT_MACROS' )


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS}")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS}")

###########################################################################################

# Add Source Code
add_subdirectory(impl)

# Add Library
get_source_iotdb(iotdb_SOURCE_FILES)
get_header_iotdb(iotdb_HEADER_FILES)
add_library(iotdb SHARED ${iotdb_SOURCE_FILES} ${iotdb_HEADER_FILES})
target_include_directories(iotdb PUBLIC "include")
target_link_libraries(iotdb ${LIBRARIES})

# Documentation
add_subdirectory(docs)

add_executable(iotNode iotNode.cpp)
target_link_libraries(iotNode iotdb)
target_include_directories(iotNode PUBLIC "include")

add_executable(iotBroker iotBroker.cpp)
target_link_libraries(iotBroker iotdb)
target_include_directories(iotBroker PUBLIC "include")

add_executable(iotMaster iotMaster.cpp)
target_link_libraries(iotMaster iotdb)
target_include_directories(iotMaster PUBLIC "include")

add_executable(iotClient iotClient.cpp)
target_link_libraries(iotClient ${LIBRARIES})
target_include_directories(iotClient PUBLIC "include")

add_executable(iotServer iotServer.cpp)
target_link_libraries(iotServer ${LIBRARIES})
target_include_directories(iotServer PUBLIC "include")

add_executable(iotRestServer iotRestServer.cpp)
target_link_libraries(iotRestServer ${LIBRARIES} iotdb)
target_include_directories(iotRestServer PUBLIC "include")

# Add tests with command
enable_testing()
add_subdirectory(tests)
