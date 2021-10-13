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
cmake_minimum_required(VERSION 3.20.0)

set(CMAKE_VERBOSE_MAKEFILE OFF)
set(VCPKG_BINARY_VERSION v6)

# for macOS or iOS, watchOS, tvOS (since 3.10.3), and other UNIXes
# The system architecture is normally set in CMAKE_HOST_PROCESSOR,
# which is set by the PROJECT command. However, we cannot call PROJECT
# at this point because we want to use a custom toolchain file.
execute_process(COMMAND uname -m OUTPUT_VARIABLE NES_HOST_PROCESSOR)

if(APPLE)
    if (NES_HOST_PROCESSOR MATCHES "x86_64")
        set(VCPKG_TARGET_TRIPLET x64-osx-nes)
    elseif(NES_HOST_PROCESSOR MATCHES "arm64")
        set(VCPKG_TARGET_TRIPLET arm64-osx-nes)
    endif()
elseif(UNIX AND NOT APPLE)
    if (NES_HOST_PROCESSOR MATCHES "x86_64")
        set(VCPKG_TARGET_TRIPLET x64-linux-nes)
    elseif(NES_HOST_PROCESSOR MATCHES "arm64" OR NES_HOST_PROCESSOR MATCHES "aarch64")
        set(VCPKG_TARGET_TRIPLET arm64-linux-nes)
    endif()
endif()

function(download_file url filename hash_type hash)
    message("Download: ${url}")
    if (NOT EXISTS ${filename})
        file(DOWNLOAD ${url} ${filename}
                TIMEOUT 0)
    endif ()
endfunction(download_file)

set(CMAKE_THREAD_LIBS_INIT "-lpthread")

# check if prebuild dependencies are already downloaded
if (CMAKE_TOOLCHAIN_FILE)
    message("Use custom toolchain file: ${CMAKE_TOOLCHAIN_FILE}.")
else ()
    set(BINARY_NAME nes-dependencies-${VCPKG_BINARY_VERSION}-${VCPKG_TARGET_TRIPLET})
    IF (NOT EXISTS ${CMAKE_CURRENT_BINARY_DIR}/${BINARY_NAME})
        message("NES dependencies do not exist!")
        download_file(https://github.com/nebulastream/dependencies/releases/download/${VCPKG_BINARY_VERSION}/${BINARY_NAME}.7z
                ${CMAKE_CURRENT_BINARY_DIR}/${BINARY_NAME}.7z
                SHA1 074e7a34c486d8956562b65a247d908ee097fb07
                )
        file(ARCHIVE_EXTRACT INPUT ${CMAKE_CURRENT_BINARY_DIR}/${BINARY_NAME}.7z DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
    endif ()

    # set toolchain file to use prebuild dependencies only if no toolchain file is already set.
    message("Set toolchain file for prebuild dir.")
    set(CMAKE_TOOLCHAIN_FILE "${CMAKE_CURRENT_BINARY_DIR}/${BINARY_NAME}/scripts/buildsystems/vcpkg.cmake")

    # In combination with prebuild binaries, NES supports self hosting its compilation with the clang binary.
    # This could be handy if the build environment has no recent compiler.
    if (NES_SELF_HOSTING)
        message(STATUS "Self-host compilation of NES")
        set(CMAKE_C_COMPILER "${CMAKE_CURRENT_BINARY_DIR}/${BINARY_NAME}/installed/${VCPKG_TARGET_TRIPLET}/tools/llvm/clang-12")
        set(CMAKE_CXX_COMPILER "${CMAKE_CURRENT_BINARY_DIR}/${BINARY_NAME}/installed/${VCPKG_TARGET_TRIPLET}/tools/llvm/clang++")
    else ()
        message(STATUS "Use system compiler")
    endif ()
    message(STATUS "Going to use ${CMAKE_CXX_COMPILER}")
endif ()

project(NES CXX)
message(STATUS "CMAKE_C_COMPILER=${CMAKE_C_COMPILER}")
message(STATUS "CMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}")
option(CMAKE_EXPORT_COMPILE_COMMANDS "Write JSON compile commands database" ON)
option(CMAKE_NES_DEBUG_TUPLE_BUFFER_LEAKS "Build using tuple buffer leak detector" OFF)
option(CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE "Build using folly and lock-free MPMC queue" ON)
option(CMAKE_USE_LATCH_FREE_BUFFER_MANAGER "Build using folly and lock-free MPMC queue for the buffer manager" ON)
set(CMAKE_USE_LATCH_FREE_BUFFER_MANAGER ON)
option(ENABLE_IWYU "Enable include-what-you-use suggestions (if found on the system)" OFF)
option(ENABLE_CLANG_TIDY OFF)
option(NES_BUILD_NATIVE "Override mtune/march to load native support" OFF)

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
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set NES Flags
set(NES_SPECIFIC_FLAGS)
if (CMAKE_NES_DEBUG_TUPLE_BUFFER_LEAKS)
    set(NES_SPECIFIC_FLAGS "-DNES_DEBUG_TUPLE_BUFFER_LEAKS=1")
endif ()

if (CMAKE_NES_DEBUG_TUPLE_BUFFER_LEAKS)
    set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_DEBUG_TUPLE_BUFFER_LEAKS=1")
endif ()

if (CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE)
    message("use CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE")
    set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE=1")
    if (CMAKE_USE_LATCH_FREE_BUFFER_MANAGER)
        message("use CMAKE_USE_LATCH_FREE_BUFFER_MANAGER")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_USE_LATCH_FREE_BUFFER_MANAGER=1")
    endif ()
endif ()

if (CMAKE_USE_ONE_QUEUE_PER_NUMA_NODE)
    set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_USE_ONE_QUEUE_PER_NUMA_NODE=1")
    if (CMAKE_USE_LATCH_FREE_BUFFER_MANAGER)
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_USE_LATCH_FREE_BUFFER_MANAGER=1")
    endif ()
endif ()

# enables tracing of stack traces if operator / expression nodes are created
if (CMAKE_NES_TRACE_NODE_CREATION)
    set(NES_SPECIFIC_FLAGS "-DNES_TRACE_NODE_CREATION=1")
endif ()

# Set NES_LOGGING_LEVEL Flag
if (NES_LOGGING_LEVEL)
    get_nes_log_level_value(NES_LOGGING_VALUE)
else (NES_LOGGING_LEVEL)
    set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -Werror=unused-variable -Werror=unused-parameter -DNES_LOGGING_TRACE_LEVEL=1")
    message("---Everything will be logged")
endif (NES_LOGGING_LEVEL)
# Set Optimization Flags
set(NES_WARNINGS "-Wall -Wextra -pedantic -Wno-null-character -Wno-dollar-in-identifier-extension -Werror=extra -Werror=exceptions -Werror=all -Werror=return-type -Werror=return-stack-address -Werror=delete-non-virtual-dtor -Werror=deprecated -Werror=writable-strings -Werror=array-bounds -Werror=ignored-qualifiers -Werror=sign-compare")
set(CMAKE_CXX_FLAGS "${NES_WARNINGS} -fcoroutines-ts -fstandalone-debug -g ${NES_SPECIFIC_FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DNES_DEBUG_MODE=1")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNES_RELEASE_MODE=1")
include(cmake/Sanitizers.cmake)

if (ENABLE_IWYU)
    find_program(iwyu_path NAMES include-what-you-use iwyu)
    if(iwyu_path)
      message(STATUS "IWYU enabled and found! Suggestions will be printed to stderr.")
    else()
      message(WARNING "IWYU is enabled but not found! You will not get any suggestions")
    endif()
endif()

if (ENABLE_CLANG_TIDY)
    message(STATUS "clang tidy enabled")
    set(CMAKE_CXX_CLANG_TIDY clang-tidy)
else()
    message(STATUS "clang tidy not enabled")
endif()

if (NES_BUILD_BENCHMARKS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNES_BENCHMARKS_FLAG_MODE=1")
    message("Build with all flags on, only use for benchmarking")
    if (NES_BENCHMARKS_DETAILED_LATENCY_MEASUREMENT)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -DNES_BENCHMARKS_DETAILED_LATENCY_MEASUREMENT=1")
        message("use latency measurement")
    endif ()
    #-fno-omit-frame-pointer
endif ()

# AVX detection
find_package(AVX)

# Native/Generic march support
if (NES_BUILD_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mtune=native -march=native -DNES_BENCHMARKS_NATIVE_MODE=1")
    if (${AVX2_FOUND})
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2")
    endif ()
else()
    # Compiler should produce specific code for system architecture
    include(CheckCXXCompilerFlag)
    CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
    if (COMPILER_SUPPORTS_MARCH_NATIVE)
        message(STATUS "CMAKE detects native arch support")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mtune=native -march=native -DNES_BENCHMARKS_NATIVE_MODE=1")
        if (${AVX2_FOUND})
            message(STATUS "CMAKE detects AVX2 support")
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2")
        endif ()
    else()
        if (CMAKE_SYSTEM_PROCESSOR MATCHES "x86-64")
            message(STATUS "CMAKE detects generic x86-64 arch support")
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=x86-64 -mtune=generic")
        elseif(APPLE AND CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
            message(STATUS "CMAKE detects APPLE ARM64 support")
            # Clang has the exact same macros for A14 as M1.
            # Current Clang master has -mcpu=apple-m1 but
            # not yet on latest release. Overall, arm64/aarch64
            # offer so many different configuration parameters
            # that compilers prefer -mcpu=XXX instead.
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcpu=apple-a14")
        elseif(NOT APPLE AND CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
            message(STATUS "CMAKE detects generic ARM64 support")
            # Arm themselves suggest using -mcpu=native, or in general,
            # to use -mcpu=CPU_TYPE. For more info, here:
            # https://community.arm.com/developer/tools-software/tools/b/tools-software-ides-blog/posts/compiler-flags-across-architectures-march-mtune-and-mcpu
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcpu=native")
        endif()
    endif ()
endif ()


message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS_DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "CMAKE_CXX_FLAGS_RELEASE: ${CMAKE_CXX_FLAGS_RELEASE}")

# folly and cpprestsdk relies on boost so we set the right configuration flag
set(Boost_NO_WARN_NEW_VERSIONS TRUE)

# jemalloc
option(CMAKE_ENABLE_JEMALLOC "Build using jemalloc" OFF)
message(STATUS "Using jemalloc: ${CMAKE_ENABLE_JEMALLOC}")
if (CMAKE_ENABLE_JEMALLOC)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(JEMALLOC jemalloc)
    pkg_search_module(JEMALLOC REQUIRED jemalloc)
    include_directories(${JEMALLOC_INCLUDE_DIRS})
    set(LIBRARIES ${LIBRARIES} ${JEMALLOC_LIBRARIES})
endif ()
# lcryto is required for asio to communicate
if(APPLE)
    find_library(SSL_LIB libssl.a REQUIRED)
    find_library(CRYPTO_LIB libcrypto.a REQUIRED)
    set(LIBRARIES ${LIBRARIES} -ldl -liconv ${CRYPTO_LIB} ${SSL_LIB})
endif()
if(UNIX AND NOT APPLE)
    set(LIBRARIES ${LIBRARIES} -ldl -lcrypto -lssl)
endif()

# folly
if (CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE)
    find_package(folly CONFIG REQUIRED)
    set(LIBRARIES ${LIBRARIES} Folly::folly Folly::folly_deps)
endif ()

if (CMAKE_USE_ONE_QUEUE_PER_NUMA_NODE)
    find_package(folly CONFIG REQUIRED)
    set(LIBRARIES ${LIBRARIES} Folly::folly Folly::folly_deps)
endif ()


# Numa awareness
find_package(NUMA)
if(NUMA_FOUND)
    set(LIBRARIES ${LIBRARIES} -lnuma)
    add_compile_definitions(NES_ENABLE_NUMA_SUPPORT)
    message(STATUS "Using Numa")
endif()

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
message(STATUS "Libzmq")
find_package(ZeroMQ CONFIG REQUIRED)
if (NOT ${ZeroMQ_FOUND})
    message(FATAL_ERROR "Unable to find libzmq")
endif ()
set(LIBRARIES ${LIBRARIES} libzmq libzmq-static)

# CppZMQ
message(STATUS "cppzmq")
find_package(cppzmq CONFIG REQUIRED)
if (NOT ${CPPZMQ_FOUND})
    message(FATAL_ERROR "Unable to find cppzmq")
endif ()
set(LIBRARIES ${LIBRARIES} cppzmq cppzmq-static)

# Z3 Solver
message(STATUS "Z3")
find_package(Z3 REQUIRED)
if (NOT ${Z3_FOUND})
    message(FATAL_ERROR "Unable to find Z3")
endif ()
set(LIBRARIES ${LIBRARIES} ${Z3_LIBRARIES})
message(STATUS "Using z3: " ${Z3_LIBRARIES})

# Log4cc
find_package(log4cxx CONFIG REQUIRED)
if (NOT ${log4cxx})
    message(FATAL_ERROR "Unable to find log4cxx")
endif ()
include_directories(${Log4cxx_INCLUDE_DIR})
set(LIBRARIES ${LIBRARIES} log4cxx)
message(STATUS "Using Log4cxx: " ${Log4cxx_LIBRARY})

# Backward
include(cmake/BackwardConfig.cmake)
include_directories(${BACKWARD_INCLUDE_DIRS})
add_compile_definitions(${BACKWARD_DEFINITIONS})
link_directories(${BACKWARD_LIBRARIES})
set(LIBRARIES ${LIBRARIES} ${BACKWARD_LIBRARIES})


# Library containing dlopen and dlcose.
set(LIBRARIES ${LIBRARIES} ${CMAKE_DL_LIBS})

# C++ REST SDK
find_package(cpprestsdk CONFIG REQUIRED)
set(LIBRARIES ${LIBRARIES} cpprestsdk::cpprest cpprestsdk::cpprestsdk_zlib_internal cpprestsdk::cpprestsdk_boost_internal cpprestsdk::cpprestsdk_openssl_internal)

# Library containing Kafka client
if (NES_USE_KAFKA)
    find_package(CppKafka CONFIG REQUIRED)
    set(LIBRARIES ${LIBRARIES} ${CppKafka_LIBRARIES})
    add_compile_definitions(ENABLE_KAFKA_BUILD)
endif (NES_USE_KAFKA)

# Library containing OPC client
if (NES_USE_OPC)
    find_package(open62541 CONFIG REQUIRED)
    set(LIBRARIES ${LIBRARIES} open62541::open62541)
    add_compile_definitions(ENABLE_OPC_BUILD)
endif (NES_USE_OPC)

# Library containing MQTT client
if (NES_USE_MQTT)
    message("VCPKG_TARGET_TRIPLET: ${VCPKG_TARGET_TRIPLET}")
    find_package(PahoMqttCpp CONFIG REQUIRED)
    set(LIBRARIES ${LIBRARIES} PahoMqttCpp::paho-mqttpp3)
    add_compile_definitions(ENABLE_MQTT_BUILD)
endif (NES_USE_MQTT)

# Eigen 3.3 linear algebra library
if (NES_USE_ADAPTIVE)
    find_package(Eigen3 CONFIG REQUIRED NO_MODULE)
    set(LIBRARIES ${LIBRARIES} Eigen3::Eigen)
    message("Adaptive sampling dependencies OK")
endif (NES_USE_ADAPTIVE)

# folly
if (CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE)
    find_dependency(gflags CONFIG REQUIRED) # required on macos
    find_package(folly CONFIG REQUIRED)
    set(LIBRARIES ${LIBRARIES} Folly::folly Folly::folly_deps)
endif ()

# Print all used include directories
message(STATUS "INCLUDE_DIRS: ${INCLUDE_DIRECTORIES}")
set(PATH_TO_NES_SOURCE_CODE ${PROJECT_SOURCE_DIR})
get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
foreach (dir ${dirs})
    message(STATUS "  - ${dir}")
endforeach ()

message(STATUS " Libraries: ${LIBRARIES}")
message(STATUS " GRPC Libraries: ${GRPC_LIBRARIES}")

# Definition of runtime variables ######################################################################################
# LLVM
find_package(LLVM 12.0.0 CONFIG REQUIRED)
if (LLVM_FOUND)
    message("-- LLVM version ${LLVM_PACKAGE_VERSION}")
    message("-- LLVM binary dir ${LLVM_BINARY_DIR}")
    message("-- LLVM tool dir ${LLVM_TOOLS_INSTALL_DIR}")
    set(CLANG_EXECUTABLE ${LLVM_BINARY_DIR}/${LLVM_TOOLS_INSTALL_DIR}/clang++)
    set(CLANG_FORMAT_EXECUTABLE ${LLVM_BINARY_DIR}/${LLVM_TOOLS_INSTALL_DIR}/clang-format)
else ()
    message(FATAL_ERROR "Cannot find llvm")
endif ()
message("-- LLVM version ${LLVM_VERSION_STRING}")
message("-- LLVM version ${LLVM_ROOT_DIR}")

# Set definition values.
set(GENERATED_CODE_DIRECTORY ${PROJECT_BINARY_DIR}/generated-code)
set(PATH_TO_NES_SOURCE_CODE ${PROJECT_SOURCE_DIR})
set(PATH_TO_DEB_SOURCE_CODE ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET})
set(PATH_TO_BINARY_DIR ${CMAKE_BINARY_DIR})
message("PATH_TO_DEB_SOURCE_CODE Dir: ${PATH_TO_DEB_SOURCE_CODE}")

# Print definition values.
message(STATUS "Clang Executable: ${CLANG_EXECUTABLE}")
message(STATUS "Clang-Format Executable: ${CLANG_FORMAT_EXECUTABLE}")
message(STATUS "Generated Code Directory: ${GENERATED_CODE_DIRECTORY}")

# Add definitions as compile flag.
add_definitions(-D 'CLANG_EXECUTABLE=\"${CLANG_EXECUTABLE}\"')
add_definitions(-D 'CLANG_FORMAT_EXECUTABLE=\"${CLANG_FORMAT_EXECUTABLE}\"')
add_definitions(-D 'GENERATED_CODE_DIRECTORY=\"${GENERATED_CODE_DIRECTORY}\"')
add_definitions(-D 'PATH_TO_NES_SOURCE_CODE=\"${PATH_TO_NES_SOURCE_CODE}\"')
add_definitions(-D 'PATH_TO_DEB_SOURCE_CODE=\"${PATH_TO_DEB_SOURCE_CODE}\"')
add_definitions(-D 'PATH_TO_BINARY_DIR=\"${PATH_TO_BINARY_DIR}\"')
if(APPLE)
    add_definitions(-D 'NES_OSX_SYSROOT=\"${CMAKE_OSX_SYSROOT}\"')
endif()

add_definitions(-D 'QUERY_COMPILATION_CC=\"${QUERY_COMPILATION_CC}\"')
add_definitions(-D 'CLANG_INCLUDE_DIRS=\"${CLANG_INCLUDE_DIRS}\"')

# fix compile error with llvm (https://stackoverflow.com/a/18966387)
add_definitions(-D '__STDC_CONSTANT_MACROS')
add_definitions(-D '__STDC_LIMIT_MACROS')


# $ORIGIN is to read the shared object from the installation directory
if(ORIGIN)
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath='$ORIGIN'")
endif()
#https://cmake.org/cmake/help/latest/variable/CMAKE_INSTALL_RPATH_USE_LINK_PATH.html
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH "true")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS}")

###########################################################################################

# Cross compilation
if (CMAKE_CROSSCOMPILING)
    set(QEMU_EMULATOR_PATH /usr/bin/qemu-aarch64)
    message(STATUS "Current Qemu Executable: ${QEMU_EMULATOR_PATH}")
    add_definitions(-D 'QEMU_EMULATOR_PATH=\"${QEMU_EMULATOR_PATH}\"')
endif ()

###########################################################################################

# Add Source Code
add_subdirectory(src)

# Add Library
get_source_nes(nes_SOURCE_FILES)
get_header_nes(nes_HEADER_FILES)
add_library(nes SHARED ${nes_SOURCE_FILES} ${nes_HEADER_FILES} ${OUTPUT_SOURCES} ${OUTPUT_HEADERS} include/Util/BufferStorageUnit.hpp)
target_include_directories(nes PUBLIC "include")
target_include_directories(nes PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(nes PUBLIC ${LIBRARIES} ${PROTOBUF_LIBRARIES})
if(iwyu_path)
    set_property(TARGET nes PROPERTY CXX_INCLUDE_WHAT_YOU_USE ${iwyu_path})
endif()


if (GRPC_LIBRARIES)
    target_link_libraries(nes PRIVATE ${GRPC_LIBRARIES})
endif ()
# Documentation
add_subdirectory(docs)

add_executable(nesCoordinator src/Executables/nesCoordinatorStarter.cpp)
target_link_libraries(nesCoordinator PRIVATE ${LIBRARIES} nes)
target_include_directories(nesCoordinator PUBLIC "include")
target_include_directories(nesCoordinator PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
if (GRPC_LIBRARIES)
    target_link_libraries(nesCoordinator PRIVATE ${GRPC_LIBRARIES})
endif ()

add_executable(nesWorker src/Executables/nesWorkerStarter.cpp)
target_link_libraries(nesWorker PUBLIC ${LIBRARIES} nes)
target_include_directories(nesWorker PUBLIC "include")
target_include_directories(nesWorker PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
if (GRPC_LIBRARIES)
    target_link_libraries(nesWorker PRIVATE ${GRPC_LIBRARIES})
endif ()

# Add tests with command
enable_testing()
add_subdirectory(tests)

#Add benchmarks with command
add_subdirectory(benchmark)

#Change to alter behaviour of cpack and installation directory of deb package
install(TARGETS nes
        COMPONENT nesLibrary
        DESTINATION lib)

install(TARGETS nesCoordinator nesWorker
        COMPONENT nesExecutables
        RUNTIME
        DESTINATION "/opt/local/nebula-stream")

install(DIRECTORY "include"
        COMPONENT nesHeaders
        DESTINATION "/nebulastream")

SET(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_VERSION ${${PROJECT_NAME}_VERSION})
set(CPACK_PACKAGE_NAME "NebulaStream")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "Team NebulaStream")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Team NebulaStream") #required
set(CPACK_PACKAGE_VENDOR "Team NebulaStream")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
set(CPACK_COMPONENTS_ALL nesExecutables nesLibrary nesHeaders)
set(CPACK_DEBIAN_PACKAGE_DEPENDS "cmake, libdwarf-dev, libdwarf1, binutils-dev, libdw-dev, libssl-dev")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")

##This allows call to ldconfig to load the configuration after installation
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_LIST_DIR}/triggers")
set(CPACK_DEBIAN_PACKAGE_CONTROL_STRICT_PERMISSION TRUE)
INCLUDE(CPack)

#Other configurations
project_enable_clang_format()
if (CMAKE_CROSSCOMPILING)
    project_enable_emulated_tests()
endif ()
project_enable_release()
project_enable_version()
