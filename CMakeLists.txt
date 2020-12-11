# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.16.0)
project(NES CXX)

if (POLICY CMP0004)
    cmake_policy(SET CMP0004 OLD)
endif ()

# Find project version
include(cmake/GetSemanticVersion.cmake)

# Update Version Automatically
#if (IS_GIT_DIRECTORY)
#    configure_file(cmake/version.hpp.in ${CMAKE_CURRENT_SOURCE_DIR}/include/Version/version.hpp)
#endif ()

# Custom CMake find instructions and macros
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")
include(cmake/macros.cmake)
include(${CMAKE_ROOT}/Modules/ExternalProject.cmake)

# C++ Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set NES Flags
set(NES_SPECIFIC_FLAGS)
if (CMAKE_NES_DEBUG_TUPLE_BUFFER_LEAKS)
    set(NES_SPECIFIC_FLAGS "-DNES_DEBUG_TUPLE_BUFFER_LEAKS=1")
endif ()

# Set Optimization Flags
set(CMAKE_CXX_FLAGS "-Wall -Werror=return-type -Werror=unused-variable -Werror=unused-parameter -g ${NES_SPECIFIC_FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DNO_RACE_CHECK -DNES_DEBUG_MODE=1")
set(CMAKE_CXX_FLAGS_RELEASE "-Wall -Wno-unused-variable -Wno-unused-parameter -Wno-ignored-qualifiers -Wno-sign-compare -O3 -g")

# Set Flamegraph Flags
if (NES_BUILD_BENCHMARKS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -fno-omit-frame-pointer")
    message("---Flags needed for retrieving function names in flamegraphs have been added!")
endif()

# Set NES_LOGGING_LEVEL Flag
if (NES_LOGGING_LEVEL)
    get_nes_log_level_value(NES_LOGGING_VALUE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNES_LOGGING_LEVEL=${NES_LOGGING_VALUE}")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DNES_LOGGING_LEVEL=${NES_LOGGING_VALUE}")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNES_LOGGING_LEVEL=${NES_LOGGING_VALUE}")
else(NES_LOGGING_LEVEL)
    message("---Everything will be logged")
endif (NES_LOGGING_LEVEL)

# Compiler should produce specific code for system architecture
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-march=x86-64" COMPILER_SUPPORTS_MARCH_NATIVE)
if (COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=x86-64 -mtune=generic")
endif ()

message(STATUS "CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE}")

# lcryto is required for asio to communicate
set(LIBRARIES ${LIBRARIES} -ldl -lcrypto -lssl)

# LLVM
find_package(LLVM 3 REQUIRED)
find_package(clang REQUIRED)

message("-- LLVM version ${LLVM_VERSION_STRING}")

# GRPC and protobuf
set(gRPC_RELEASE "v1.28.1")
# This assumes that gRPC and all its dependencies are already installed
# on this system, so they can be located by find_package().

# Find Protobuf installation
# Looks for protobuf-config.cmake file installed by Protobuf's cmake installation.
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf REQUIRED)
message(STATUS "Using protobuf ${protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)
if (CMAKE_CROSSCOMPILING)
    find_program(_PROTOBUF_PROTOC protoc)
else ()
    set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif ()

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)
if (CMAKE_CROSSCOMPILING)
    find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else ()
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif ()
set(GRPC_LIBRARIES ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

# Proto file
set(PROTO_PATH "${PROJECT_SOURCE_DIR}/grpc")
set(PROTO_FILE_PATH "${PROTO_PATH}/*.proto")

file(GLOB PROTO_FILES "${PROTO_FILE_PATH}")
foreach (PROTO_FILE ${PROTO_FILES})
    get_filename_component(FILENAME ${PROTO_FILE} "NAME")
    string(REGEX REPLACE "[.]proto$" ".pb.cc" OUTPUT_SOURCE ${CMAKE_CURRENT_BINARY_DIR}/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
    string(REGEX REPLACE "[.]proto$" ".pb.h" OUTPUT_SOURCE ${CMAKE_CURRENT_BINARY_DIR}/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
    string(REGEX REPLACE "[.]proto$" ".grpc.pb.cc" OUTPUT_SOURCE ${CMAKE_CURRENT_BINARY_DIR}/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
    string(REGEX REPLACE "[.]proto$" ".grpc.pb.h" OUTPUT_SOURCE ${CMAKE_CURRENT_BINARY_DIR}/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
endforeach ()

message(STATUS "Found the following Proto files: " ${OUTPUT_SOURCES})

add_custom_command(
        OUTPUT ${OUTPUT_SOURCES}
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${PROTO_PATH}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${PROTO_FILE_PATH}"
        DEPENDS "${PROTO_FILE_PATH}")

# LibZMQ
# ------
message(STATUS "Libzmq")
set(ZMQ_RELEASE "4.3.2")
find_package(ZMQ ${ZMQ_RELEASE} REQUIRED)
set(LIBRARIES ${LIBRARIES} libzmq)

# Z3 Solver
message(STATUS "Z3")
find_package(Z3 REQUIRED)
if(NOT ${Z3_FOUND})
    message(FATAL_ERROR "Unable to find Z3")
endif()
set(LIBRARIES ${LIBRARIES} ${Z3_LIBRARIES})

# CppZMQ
message(STATUS "cppzmq")
set(CPPZMQ_RELEASE "4.6.0")
find_package(cppzmq ${CPPZMQ_RELEASE} REQUIRED)

# Log4cc
find_package(Log4cxx REQUIRED)
include_directories(${Log4cxx_INCLUDE_DIR})
set(LIBRARIES ${LIBRARIES} ${Log4cxx_LIBRARY})

if (LOG4CXX_FOUND)
    message(STATUS "LOG4CXX found")
else (LOG4CXX_FOUND)
    message(STATUS "LOG4CXX not found")
    message(STATUS "On Ubuntu, install the package by the following command!")
    message(STATUS "sudo apt-get install liblog4cxx10-dev")
endif (LOG4CXX_FOUND)

# Boost Libraries
set(Boost_USE_MULTITHREADED TRUE)
find_package(Boost 1.65.1 REQUIRED system thread program_options filesystem chrono) # Only check if lib is available on system for generated code.
link_directories(${Boost_LIBRARY_DIRS})
include_directories(${Boost_INCLUDE_DIRS})
set(LIBRARIES ${LIBRARIES} ${Boost_LIBRARIES})

# Backward

include(cmake/BackwardConfig.cmake)
include_directories(${BACKWARD_INCLUDE_DIRS})
add_compile_definitions(${BACKWARD_DEFINITIONS})
link_directories(${BACKWARD_LIBRARIES})
set(LIBRARIES ${LIBRARIES} ${BACKWARD_LIBRARIES})


# Library containing dlopen and dlcose.
set(LIBRARIES ${LIBRARIES} ${CMAKE_DL_LIBS})

# C++ REST SDK
set(cpprestsdk_DIR ${CPPRESTSDK_DIR})
message("Found cpprestsdk in ${cpprestsdk_DIR}")
find_package(cpprestsdk REQUIRED)
# Library containing rest dependencies
set(LIBRARIES ${LIBRARIES} cpprestsdk::cpprest)

# Library containing actor framework
#find_package(CAF COMPONENTS core io)
#set(LIBRARIES ${LIBRARIES} ${CAF_EXTRA_LDFLAGS} ${CAF_LIBRARIES} ${PTHREAD_LIBRARIES})

# Library containing Kafka client
if (NES_USE_KAFKA)
    find_package(CppKafka REQUIRED)
    set(LIBRARIES ${LIBRARIES} ${CppKafka_LIBRARIES})
    add_compile_definitions(ENABLE_KAFKA_BUILD)
endif (NES_USE_KAFKA)

# Library containing OPC client
if (NES_USE_OPC)
    find_package(open62541 REQUIRED COMPONENTS FullNamespace)
    set(LIBRARIES ${LIBRARIES} open62541::open62541)
    add_compile_definitions(ENABLE_OPC_BUILD)
endif (NES_USE_OPC)

# Eigen 3.3 linear algebra library
if (NES_USE_ADAPTIVE)
    find_package(Eigen3 3.3 REQUIRED NO_MODULE)
    set(LIBRARIES ${LIBRARIES} Eigen3::Eigen)
    message("Adaptive sampling dependencies OK")
endif (NES_USE_ADAPTIVE)

# Print all used include directories
message(STATUS "INCLUDE_DIRS:")
get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
foreach (dir ${dirs})
    message(STATUS "  - ${dir}")
endforeach ()

message(STATUS " Libraries: ${LIBRARIES}")
message(STATUS " GRPC Libraries: ${GRPC_LIBRARIES}")

# Definition of runtime variables ######################################################################################

# Set definition values.
set(CLANG_EXECUTABLE ${LLVM_ROOT_DIR}/bin/clang++)
set(CLANG_FORMAT_EXECUTABLE ${LLVM_ROOT_DIR}/bin/clang-format)
set(GENERATED_CODE_DIRECTORY ${PROJECT_BINARY_DIR}/generated-code)
set(PATH_TO_NES_SOURCE_CODE ${PROJECT_SOURCE_DIR})

# Print definition values.
message(STATUS "Clang Executable: ${CLANG_EXECUTABLE}")
message(STATUS "Clang-Format Executable: ${CLANG_FORMAT_EXECUTABLE}")
message(STATUS "Generated Code Directory: ${GENERATED_CODE_DIRECTORY}")

# Add definitions as compile flag.
add_definitions(-D 'CLANG_EXECUTABLE=\"${CLANG_EXECUTABLE}\"')
add_definitions(-D 'CLANG_FORMAT_EXECUTABLE=\"${CLANG_FORMAT_EXECUTABLE}\"')
add_definitions(-D 'GENERATED_CODE_DIRECTORY=\"${GENERATED_CODE_DIRECTORY}\"')
add_definitions(-D 'PATH_TO_NES_SOURCE_CODE=\"${PATH_TO_NES_SOURCE_CODE}\"')

add_definitions(-D 'QUERY_COMPILATION_CC=\"${QUERY_COMPILATION_CC}\"')
add_definitions(-D 'CLANG_INCLUDE_DIRS=\"${CLANG_INCLUDE_DIRS}\"')

# fix compile error with llvm (https://stackoverflow.com/a/18966387)
add_definitions(-D '__STDC_CONSTANT_MACROS')
add_definitions(-D '__STDC_LIMIT_MACROS')


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
# $ORIGIN is to read the shared object from the installation directory
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath='$ORIGIN'")
#https://cmake.org/cmake/help/latest/variable/CMAKE_INSTALL_RPATH_USE_LINK_PATH.html
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH "true")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS}")

###########################################################################################

# Add Source Code
add_subdirectory(src)

# Add Library
get_source_nes(nes_SOURCE_FILES)
get_header_nes(nes_HEADER_FILES)
add_library(nes SHARED ${nes_SOURCE_FILES} ${nes_HEADER_FILES} ${OUTPUT_SOURCES} ${OUTPUT_HEADERS})
target_include_directories(nes PUBLIC "include")
target_include_directories(nes PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(nes PUBLIC ${LIBRARIES} cppzmq ${PROTOBUF_LIBRARIES})
if (GRPC_LIBRARIES)
    target_link_libraries(nes PRIVATE ${GRPC_LIBRARIES})
endif ()
# Documentation
add_subdirectory(docs)

add_executable(nesCoordinator src/Executables/nesCoordinatorStarter.cpp)
target_link_libraries(nesCoordinator PUBLIC ${LIBRARIES} nes)
target_include_directories(nesCoordinator PUBLIC "include")
target_include_directories(nesCoordinator PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
if (GRPC_LIBRARIES)
    target_link_libraries(nesCoordinator PRIVATE ${GRPC_LIBRARIES})
endif ()
set_target_properties(nesCoordinator PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})


add_executable(nesWorker src/Executables/nesWorkerStarter.cpp)
target_link_libraries(nesWorker PUBLIC ${LIBRARIES} nes)
target_include_directories(nesWorker PUBLIC "include")
target_include_directories(nesWorker PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
if (GRPC_LIBRARIES)
    target_link_libraries(nesWorker PRIVATE ${GRPC_LIBRARIES})
endif ()
set_target_properties(nesWorker PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})


# Add tests with command
enable_testing()
add_subdirectory(tests)

#Add benchmarks with command
add_subdirectory(benchmark)

install(TARGETS nesCoordinator nesWorker nes
        COMPONENT NebulaStream
        RUNTIME DESTINATION "/opt/local/nebula-stream"
        LIBRARY DESTINATION "/opt/local/nebula-stream"
        DESTINATION "/opt/local/nebula-stream"
        )

install(DIRECTORY "include" DESTINATION "/nebulastream" COMPONENT NebulaStream)

SET(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_VERSION ${${PROJECT_NAME}_VERSION})
set(CPACK_PACKAGE_NAME "NebulaStream")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "Team NebulaStream")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Team NebulaStream") #required
set(CPACK_PACKAGE_VENDOR "Team NebulaStream")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
set(CPACK_DEBIAN_PACKAGE_DEPENDS "cmake, LLVM, clang, libdwarf-dev, libdwarf1, llvm-dev, binutils-dev, libdw-dev, libboost-all-dev, liblog4cxx-dev, libcpprest-dev, libssl-dev, clang-format")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")
INCLUDE(CPack)

project_enable_clang_format()
project_enable_release()
project_enable_version()
