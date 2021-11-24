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
set(VCPKG_BINARY_VERSION v7)

# Activate self hosting, such that the compiler from the dependencies is taken
option(NES_SELF_HOSTING "use self hosting compiler" ON)
# Enable native mtune/march for optimizations
option(NES_BUILD_NATIVE "Override mtune/march to load native support" OFF)
# Enable local dependency building (this may take a long time)
option(NES_BUILD_DEPENDENCIES_LOCAL "Build all dependencies locally" OFF)
option(CMAKE_EXPORT_COMPILE_COMMANDS "Write JSON compile commands database" ON)
option(CMAKE_NES_DEBUG_TUPLE_BUFFER_LEAKS "Build using tuple buffer leak detector" OFF)
option(CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE "Build using folly and lock-free MPMC queue" ON)
option(CMAKE_USE_LATCH_FREE_BUFFER_MANAGER "Build using folly and lock-free MPMC queue for the buffer manager" ON)
option(CMAKE_USE_PAPI_PROFILER "Build using PAPI Profiler" OFF)
option(ENABLE_IWYU "Enable include-what-you-use suggestions (if found on the system)" OFF)
option(ENABLE_CLANG_TIDY OFF)

# for macOS or iOS, watchOS, tvOS (since 3.10.3), and other UNIXes
# The system architecture is normally set in CMAKE_HOST_PROCESSOR,
# which is set by the PROJECT command. However, we cannot call PROJECT
# at this point because we want to use a custom toolchain file.
execute_process(COMMAND uname -m OUTPUT_VARIABLE NES_HOST_PROCESSOR)

if (APPLE)
    if (NES_HOST_PROCESSOR MATCHES "x86_64")
        set(VCPKG_TARGET_TRIPLET x64-osx-nes)
    elseif (NES_HOST_PROCESSOR MATCHES "arm64")
        set(VCPKG_TARGET_TRIPLET arm64-osx-nes)
    endif ()
elseif (UNIX AND NOT APPLE)
    if (NES_HOST_PROCESSOR MATCHES "x86_64")
        set(VCPKG_TARGET_TRIPLET x64-linux-nes)
    elseif (NES_HOST_PROCESSOR MATCHES "arm64" OR NES_HOST_PROCESSOR MATCHES "aarch64")
        set(VCPKG_TARGET_TRIPLET arm64-linux-nes)
    endif ()
endif ()

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
    message(STATUS "Use custom toolchain file: ${CMAKE_TOOLCHAIN_FILE}.")
elseif (NES_BUILD_DEPENDENCIES_LOCAL)
    message(STATUS "Build Dependencies locally")
    include(FetchContent)
    FetchContent_Declare(
            nesdebs
            GIT_REPOSITORY https://github.com/nebulastream/nebulastream-dependencies.git
            #GIT_TAG ${VCPKG_BINARY_VERSION}
            GIT_TAG origin/fix-VCPKG_POLICY_ALLOW_RESTRICTED_HEADERS
    )
    FetchContent_Populate(nesdebs)
    set(CMAKE_TOOLCHAIN_FILE ${nesdebs_SOURCE_DIR}/../vcpkg/scripts/buildsystems/vcpkg.cmake
            CACHE STRING "CMake toolchain file")
    set(VCPKG_MANIFEST_DIR ${nesdebs_SOURCE_DIR} CACHE STRING "vcpkg manifest dir")
    set(VCPKG_OVERLAY_TRIPLETS ${nesdebs_SOURCE_DIR}/custom-triplets/ CACHE STRING "CMake toolchain file")
    set(NES_DEPENDENCIES_BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../${BINARY_NAME}/vcpkg_installed/${VCPKG_TARGET_TRIPLET})
else ()
    message(STATUS "Use prebuild dependencies")
    set(BINARY_NAME nes-dependencies-${VCPKG_BINARY_VERSION}-${VCPKG_TARGET_TRIPLET})
    IF (NOT EXISTS ${CMAKE_CURRENT_BINARY_DIR}/../${BINARY_NAME})
        message(STATUS "NES dependencies do not exist!")
        download_file(https://github.com/nebulastream/dependencies/releases/download/${VCPKG_BINARY_VERSION}/${BINARY_NAME}.7z
                ${CMAKE_CURRENT_BINARY_DIR}/../${BINARY_NAME}.7z
                SHA1 074e7a34c486d8956562b65a247d908ee097fb07
                )
        file(ARCHIVE_EXTRACT INPUT ${CMAKE_CURRENT_BINARY_DIR}/../${BINARY_NAME}.7z DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/../)
    endif ()

    # set toolchain file to use prebuild dependencies only if no toolchain file is already set.
    message(STATUS "Set toolchain file for prebuild dir.")
    set(CMAKE_TOOLCHAIN_FILE "${CMAKE_CURRENT_BINARY_DIR}/../${BINARY_NAME}/scripts/buildsystems/vcpkg.cmake")
    set(NES_DEPENDENCIES_BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../${BINARY_NAME}/installed/${VCPKG_TARGET_TRIPLET})
endif ()

# NES supports self hosting its compilation with the local clang binary.
if (NES_SELF_HOSTING)
    message(STATUS "Self-host compilation of NES from ${NES_DEPENDENCIES_BINARY_ROOT}")
    set(CMAKE_C_COMPILER "${NES_DEPENDENCIES_BINARY_ROOT}/tools/llvm/clang-12")
    set(CMAKE_CXX_COMPILER "${NES_DEPENDENCIES_BINARY_ROOT}/tools/llvm/clang++")
else ()
    message(STATUS "Use system compiler")
endif ()

project(NES CXX)
set(VCPKG_POLICY_ALLOW_RESTRICTED_HEADERS enabled)
message(STATUS "Going to use ${CMAKE_CXX_COMPILER}")

if (POLICY CMP0004)
    cmake_policy(SET CMP0004 OLD)
endif ()

# Log4cc
find_package(log4cxx CONFIG REQUIRED)
if (NOT ${log4cxx})
    message(FATAL_ERROR "Unable to find log4cxx")
endif ()
include_directories(${Log4cxx_INCLUDE_DIR})
set(LIBRARIES ${LIBRARIES} log4cxx)
message(STATUS "Using Log4cxx: " ${Log4cxx_LIBRARY})

if (NES_LOGGING_LEVEL)
    get_nes_log_level_value(NES_LOGGING_VALUE)
else (NES_LOGGING_LEVEL)
    set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -Werror=unused-variable -Werror=unused-parameter -DNES_LOGGING_TRACE_LEVEL=1")
    message("---Everything will be logged")
endif (NES_LOGGING_LEVEL)

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

# Set Optimization Flags
#set(NES_WARNINGS "-Wall -Wextra -pedantic -Wno-null-character -Wno-dollar-in-identifier-extension -Werror=extra -Werror=exceptions -Werror=all -Werror=return-type -Werror=return-stack-address -Werror=delete-non-virtual-dtor -Werror=deprecated -Werror=writable-strings -Werror=array-bounds -Werror=ignored-qualifiers -Werror=sign-compare")
set(CMAKE_CXX_FLAGS "${NES_WARNINGS} -fcoroutines-ts -fstandalone-debug -g ${NES_SPECIFIC_FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DNES_DEBUG_MODE=1")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNES_RELEASE_MODE=1")
include(../cmake/Sanitizers.cmake)

# Find project version
include(../cmake/GetSemanticVersion.cmake)

# Update Version Automatically
#if (IS_GIT_DIRECTORY)
#    configure_file(cmake/version.hpp.in ${CMAKE_CURRENT_SOURCE_DIR}/include/Version/version.hpp)
#endif ()

# Custom CMake find instructions and macros
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")
include(../cmake/macros.cmake)
include(${CMAKE_ROOT}/Modules/ExternalProject.cmake)

# C++ Standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set NES Flags
set(NES_SPECIFIC_FLAGS)
if (CMAKE_NES_DEBUG_TUPLE_BUFFER_LEAKS)
    set(NES_SPECIFIC_FLAGS "-DNES_DEBUG_TUPLE_BUFFER_LEAKS=1")
endif ()

# Set Sources and Headers
add_subdirectory(src)
get_source_nes_client(nes_SOURCE_FILES)
get_header_nes_client(nes_HEADER_FILES)

# Set NES Lib
file(GLOB LIBNES "../cmake-build-debug/libnes.*")
message("Found libnes: " ${LIBNES})

# Add Library
add_library(nes_client SHARED ${nes_SOURCE_FILES} ${nes_HEADERS_FILES})
target_include_directories(nes_client PUBLIC "../" "../include")
target_link_libraries(nes_client PUBLIC ${LIBNES})
if (GRPC_LIBRARIES)
    target_link_libraries(nes_client PRIVATE ${GRPC_LIBRARIES})
endif ()

#Change to alter behaviour of cpack and installation directory of deb package
install(TARGETS nes_client
        COMPONENT nesLibrary
        DESTINATION lib)

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
#project_enable_clang_format()
#if (CMAKE_CROSSCOMPILING)
#    project_enable_emulated_tests()
#endif ()
#project_enable_release()
#project_enable_version()
