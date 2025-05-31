# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.21.0)
include(CMakeDependentOption)

set(CMAKE_VERBOSE_MAKEFILE OFF)
set(LLVM_TOOLCHAIN_VERSION 19)

# Enable native mtune/march for optimizations
option(NES_BUILD_NATIVE "Override mtune/march to load native support (might improve performance significantly)" OFF)
option(CMAKE_EXPORT_COMPILE_COMMANDS "Write JSON compile commands database" ON)
option(ENABLE_IWYU "Enable include-what-you-use suggestions (if found on the system)" OFF)
option(ENABLE_FTIME_TRACE "Enable ftime-trace as a compilation flag to profile the compiler" OFF)
option(CODE_COVERAGE "Compute test coverage" OFF)
option(NES_ENABLES_TESTS "Enable tests" ON)
option(NES_ENABLE_PRECOMPILED_HEADERS "Enable precompiled headers (might improve compilation time)" OFF)
option(NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR "Enables the MLIR backend." ON)
option(NES_LOG_WITH_STACKTRACE "Log exceptions with stacktrace" ON)
option(ENABLE_LARGE_TESTS "Runs testcases with larger input data" OFF)
option(CMAKE_NES_DEBUG_TUPLE_BUFFER_LEAKS "Heavyweight instrumentation for Tuplebuffer debugging" OFF)


if (CMAKE_BUILD_TYPE STREQUAL "Benchmark")
    add_compile_definitions(NO_ASSERT)
    set(NES_BUILD_NATIVE ON)
    set(NES_LOG_LEVEL "LEVEL_NONE" CACHE STRING "LogLevel")
    message(STATUS "Building without any asserts, invariants or preconditions enabled!")
    message(STATUS "NES_BUILD_NATIVE enabling by default in Benchmark!")
    message(STATUS "Setting NES_LOG_LEVEL to LEVEL_NONE in Benchmark!")
elseif (CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_definitions(NO_ASSERT)
    if (NOT DEFINED NES_LOG_LEVEL)
        set(NES_LOG_LEVEL "ERROR" CACHE STRING "LogLevel")
        message(STATUS "NES_LOG_LEVEL was not set, therefore setting NES_LOG_LEVEL to ERROR in Release!")
    endif ()
elseif (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    if (NOT DEFINED NES_LOG_LEVEL)
        set(NES_LOG_LEVEL "WARN" CACHE STRING "LogLevel")
        message(STATUS "NES_LOG_LEVEL was not set, therefore setting NES_LOG_LEVEL to WARN in Release!")
    endif ()
else ()
    # Default is to compile all log levels
    set(NES_LOG_LEVEL "TRACE" CACHE STRING "LogLevel")
endif ()


if (NES_LOG_WITH_STACKTRACE)
    add_compile_definitions(NES_LOG_WITH_STACKTRACE)
endif ()

set(CMAKE_THREAD_LIBS_INIT "-lpthread")
include(cmake/Sanitizers.cmake)
include(cmake/ImportDependencies.cmake)
project(NES LANGUAGES C CXX)
set(VCPKG_POLICY_ALLOW_RESTRICTED_HEADERS enabled)
message(STATUS "Going to use ${CMAKE_CXX_COMPILER}")

# Emit warning if non clang-based compiler is used.
if (NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(WARNING "This project only supports clang. You are using ${CMAKE_CXX_COMPILER_ID} in version ${CMAKE_CXX_COMPILER_VERSION}")
endif ()

# Custom CMake find instructions and macros
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")
include(cmake/macros.cmake)
include(${CMAKE_ROOT}/Modules/ExternalProject.cmake)

# C++ Standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION OFF)

# Set NES Flags
set(NES_SPECIFIC_FLAGS)
if (CMAKE_NES_DEBUG_TUPLE_BUFFER_LEAKS)
    set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_DEBUG_TUPLE_BUFFER_LEAKS=1")
endif ()

# Infers the log level based on the provided "NES_LOG_LEVEL" flag.
# Currently we support the following log levels:
# TRACE, DEBUG, INFO, WARN, ERROR, FATAL_ERROR, and NONE.
# Notice that these is a compilation time provided value.
# Consequently, it is not possible to activate at runtime a log level,
# which is lower then the log level that is provided here.
set_nes_log_level_value(NES_LOGGING_VALUE)

# Set Optimization Flags
set(NES_WARNINGS "-Wall -Wconstant-conversion -Wbitwise-instead-of-logical -Wextra -pedantic -Wno-null-character -Wno-dollar-in-identifier-extension -Werror=extra -Werror=exceptions -Werror=all -Werror=integer-overflow -Werror=return-type -Werror=return-stack-address -Werror=delete-non-virtual-dtor -Werror=deprecated -Werror=writable-strings -Werror=array-bounds -Werror=ignored-qualifiers -Werror=sign-compare -Wno-deprecated-copy-with-dtor -Wno-unused-variable -Wno-unused-but-set-variable -Wno-deprecated-declarations -Wno-invalid-offsetof ")
set(CMAKE_CXX_FLAGS "${NES_WARNINGS} -fno-omit-frame-pointer ${NES_SPECIFIC_FLAGS}")

# setup rpath for linker
set(CMAKE_BUILD_RPATH "${CMAKE_BINARY_DIR}/nes-plugins")
set(CMAKE_INSTALL_RPATH "$ORIGIN/nes-plugins")

if (ENABLE_FTIME_TRACE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftime-trace")
    message(STATUS "Use ftime-trace")
endif ()


include(cmake/Sanitizers.cmake)
include(cmake/CheckToolchain.cmake)
include(cmake/UseCCache.cmake)
include(cmake/UseMold.cmake)

if (ENABLE_IWYU)
    find_program(iwyu_path NAMES include-what-you-use iwyu)
    if (iwyu_path)
        message(STATUS "IWYU enabled and found! Suggestions will be printed to stderr.")
        set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
        set(CMAKE_C_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
    else ()
        message(WARNING "IWYU is enabled but not found! You will not get any suggestions")
    endif ()
endif ()

# Native/Generic march support
if (NES_BUILD_NATIVE)
    include(CheckCXXCompilerFlag)
    CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
    if (COMPILER_SUPPORTS_MARCH_NATIVE)
        message(STATUS "CMAKE detects native arch support")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mtune=native -march=native -DNES_BENCHMARKS_NATIVE_MODE=1")
        # AVX detection
        find_package(AVX)
        if (${AVX2_FOUND})
            message(STATUS "CMAKE detects AVX2 support")
            add_compile_definitions(HAS_AVX)
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2")
        endif ()
    else ()
        message(FATAL_ERROR "NES_BUILD_NATIVE was true but the compiler does not support it on this architecture.")
    endif ()
else ()
    # Compiler should produce specific code for system architecture
    if (CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
        message(STATUS "CMAKE detects generic x86_64 arch support")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=x86-64 -mtune=generic")
    elseif (APPLE AND CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
        message(STATUS "CMAKE detects APPLE ARM64 support")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcpu=apple-m1")
    elseif (NOT APPLE AND CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
        message(STATUS "CMAKE detects generic ARM64 support")
        # Arm themselves suggest using -mcpu=native, or in general,
        # to use -mcpu=CPU_TYPE. For more info, here:
        # https://community.arm.com/developer/tools-software/tools/b/tools-software-ides-blog/posts/compiler-flags-across-architectures-march-mtune-and-mcpu
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=armv8-a")
    else ()
        message(FATAL_ERROR "CMAKE_SYSTEM_PROCESSOR was ${CMAKE_SYSTEM_PROCESSOR} this is a unsupported architecture.")
    endif ()
endif ()


message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS_DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "CMAKE_CXX_FLAGS_RELEASE: ${CMAKE_CXX_FLAGS_RELEASE}")
message(STATUS "CMAKE_CXX_FLAGS_BENCHMARK: ${CMAKE_CXX_FLAGS_BENCHMARK}")


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

# Library containing dlopen and dlcose.
set(LIBRARIES ${LIBRARIES} ${CMAKE_DL_LIBS})

find_package(nlohmann_json REQUIRED)
set(LIBRARIES ${LIBRARIES} nlohmann_json::nlohmann_json)

if (NES_ENABLES_TESTS)
    find_package(GTest CONFIG REQUIRED)
    if (NOT ${GTest_FOUND})
        message(FATAL_ERROR "Unable to find GTest")
    endif ()
    set(GTEST_LIBRARIES ${GTEST_LIBRARIES} GTest::gtest GTest::gtest_main GTest::gmock GTest::gmock_main)
endif ()

message(STATUS " Libraries: ${LIBRARIES}")
message(STATUS " GRPC Libraries: ${GRPC_LIBRARIES}")

# Definition of runtime variables ######################################################################################

# $ORIGIN is to read the shared object from the installation directory
if (ORIGIN)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath='$ORIGIN'")
endif ()
#https://cmake.org/cmake/help/latest/variable/CMAKE_INSTALL_RPATH_USE_LINK_PATH.html
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH "true")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS}")

# Add tests with command
include(cmake/NebulaStreamTest.cmake)
if (NES_ENABLES_TESTS)
    set(GTEST_LIBRARIES ${GTEST_LIBRARIES} ${LIBRARIES})
    # Add tests with command
    enable_testing()
    message(STATUS "Tests are enabled")
    if (CODE_COVERAGE)
        include(cmake/CodeCoverage.cmake)
        add_code_coverage_all_targets(EXCLUDE ${CMAKE_BINARY_DIR}/* tests/*)
        message(STATUS "Tests are enabled with code coverage")
    endif ()
else ()
    message(STATUS "Tests are disabled")
endif ()

add_custom_target(build_all_plugins)

# Add target for common lib, which contains a minimal set
# of shared functionality used by all components of nes
file(GLOB NES_DIRECTORIES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "nes-*")
foreach (NES_DIR ${NES_DIRECTORIES})
    add_subdirectory(${NES_DIR})
endforeach ()

# Add all targets in benchmarks
add_subdirectory(benchmarks)

# Other configurations
project_enable_format()

# Adding targets for code coverage if it is enabled
if (CODE_COVERAGE)
    file(GLOB NES_DIRECTORIES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "nes-*")
    list(REMOVE_ITEM NES_DIRECTORIES "nes-systests")
    list(REMOVE_ITEM NES_DIRECTORIES "nes-nebuli")
    list(REMOVE_ITEM NES_DIRECTORIES "nes-single-node-worker")
    # 'nes-optional-plugins' is not a target on its own, the plugins are part of other targets
    list(REMOVE_ITEM NES_DIRECTORIES "nes-plugins")

    foreach (NES_DIR ${NES_DIRECTORIES})
        target_code_coverage(${NES_DIR} PUBLIC AUTO ALL)
    endforeach ()
    target_code_coverage(nes-single-node-worker-lib PUBLIC AUTO ALL)
    target_code_coverage(nes-nebuli-lib PUBLIC AUTO ALL)
endif ()
