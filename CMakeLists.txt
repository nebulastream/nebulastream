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
set(LLVM_VERSION 18.1.6)
set(CLANG_FORMAT_MAJOR_VERSION 18)
# Set the not allowed comment style as voted in https://github.com/nebulastream/nebulastream-public/discussions/18
# The regex must match all comments that are not allowed in the codebase per line.
set(NOT_ALLOWED_COMMENT_STYLE_REGEX "([^\\/:]|^)(\\/\\/)+[^\\/]")

#toggle import and usage of s2 library
option(CMAKE_EXPORT_COMPILE_COMMANDS "Write JSON compile commands database" ON)
option(CMAKE_NES_DEBUG_TUPLE_BUFFER_LEAKS "Build using tuple buffer leak detector" OFF)
option(CMAKE_USE_ONE_QUEUE_PER_NUMA_NODE "Build using NUMA optimization" OFF)
option(ENABLE_IWYU "Enable include-what-you-use suggestions (if found on the system)" OFF)
option(ENABLE_FTIME_TRACE "Enable ftime-trace as a compilation flag to profile the compiler" OFF)
option(NES_COMPUTE_COVERAGE "Compute test coverage" OFF)
option(NES_ENABLES_TESTS "Compute test coverage" ON)
option(NES_CODE_COVERAGE "Enable Code Coverage" OFF)
option(NES_ENABLE_PRECOMPILED_HEADERS "Enable Precompiled Headers" OFF)
option(NES_ENABLE_EXPERIMENTAL_EXECUTION_ENGINE "Enables the experimental execution engine (Nautilus)." ON)
option(NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR "Enables the MLIR backend." ON)
option(NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR_INLINING "Enables the inlining for the MLIR backend." OFF)
option(NES_BUILD_BENCHMARKS "Build benchmarks" ON)
option(NES_BUILD_MULTITHREADED "Build multithreaded query execution tests" ON)
option(BUILD_SHARED_LIBS "Build using shared libraries" ON)

option(NES_BUILD_JOIN_MULTITHREADED "Build join multithreaded test" OFF)

set(NES_TEST_PARALLELISM 1 CACHE STRING "Set test parallelism")
set(NES_LOG_LEVEL "TRACE" CACHE STRING "LogLevel")

if (NOT NES_TEST_PARALLELISM MATCHES "^[0-9]+$")
    set(NES_TEST_PARALLELISM "1")
endif ()

set(CMAKE_THREAD_LIBS_INIT "-lpthread")
include(cmake/ImportDependencies.cmake)
include(cmake/Sanitizers.cmake)
project(NES LANGUAGES C CXX)
set(VCPKG_POLICY_ALLOW_RESTRICTED_HEADERS enabled)
message(STATUS "Going to use ${CMAKE_CXX_COMPILER}")

# Emit warning if non clang-based compiler is used.
if (NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(WARNING "This project only supports clang. You are using ${CMAKE_CXX_COMPILER_ID} in version ${CMAKE_CXX_COMPILER_VERSION}")
endif ()

# Find project version
include(cmake/semver/GetSemanticVersion.cmake)

# Custom CMake find instructions and macros
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")
include(cmake/macros.cmake)
include(${CMAKE_ROOT}/Modules/ExternalProject.cmake)

# C++ Standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION OFF)

if (CMAKE_NES_DEBUG_TUPLE_BUFFER_LEAKS)
    add_compile_definitions(NES_DEBUG_TUPLE_BUFFER_LEAKS=1)
endif ()

# Infers the log level based on the provided "NES_LOG_LEVEL" flag.
# Currently we support the following log levels:
# TRACE, DEBUG, INFO, WARN, ERROR, FATAL_ERROR, and NONE.
# Notice that these is a compilation time provided value.
# Consequently, it is not possible to activate at runtime a log level,
# which is lower then the log level that is provided here.
get_nes_log_level_value(NES_LOGGING_VALUE)

# Set Optimization Flags
set(NES_WARNINGS "-Wall -Wconstant-conversion -Wbitwise-instead-of-logical -Wextra -pedantic -Wno-null-character -Wno-dollar-in-identifier-extension -Werror=extra -Werror=exceptions -Werror=all -Werror=integer-overflow -Werror=return-type -Werror=return-stack-address -Werror=delete-non-virtual-dtor -Werror=deprecated -Werror=writable-strings -Werror=array-bounds -Werror=ignored-qualifiers -Werror=sign-compare -Wno-deprecated-copy-with-dtor -Wno-unused-variable -Wno-unused-but-set-variable -Wno-deprecated-declarations -Wno-invalid-offsetof ")
set(CMAKE_CXX_FLAGS "${NES_WARNINGS} -fno-omit-frame-pointer -fstandalone-debug -fdebug-default-version=4 -g")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DNES_DEBUG_MODE=1")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g -fno-omit-frame-pointer -DNES_RELEASE_MODE=1")

if (ENABLE_FTIME_TRACE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftime-trace")
    message(STATUS "Use ftime-trace")
endif ()

include(cmake/Sanitizers.cmake)
include(cmake/UseLibcxx.cmake)
include(cmake/UseCCache.cmake)
include(cmake/UseMold.cmake)
include(cmake/Native.cmake)

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

if (NES_BUILD_BENCHMARKS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNES_BENCHMARKS_FLAG_MODE=1")
    message("Build with all flags on, only use for benchmarking")
    if (NES_BENCHMARKS_DETAILED_LATENCY_MEASUREMENT)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -DNES_BENCHMARKS_DETAILED_LATENCY_MEASUREMENT=1")
        message("use latency measurement")
    endif ()
    #-fno-omit-frame-pointer
endif ()

message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS_DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "CMAKE_CXX_FLAGS_RELEASE: ${CMAKE_CXX_FLAGS_RELEASE}")

# folly relies on boost so we set the right configuration flag
set(Boost_NO_WARN_NEW_VERSIONS TRUE)
# Definition of runtime variables ######################################################################################

# Add tests with command
include(cmake/NebulaStreamTest.cmake)
if (NES_ENABLES_TESTS)
    # Add tests with command
    enable_testing()
    message(STATUS "Tests are enabled")
    if (NES_CODE_COVERAGE)
        set(CODE_COVERAGE ON)
        include(cmake/CodeCoverage.cmake)
        enable_extra_test_features()
        message(STATUS "Tests are enabled with code coverage")
    endif ()
else ()
    message(STATUS "Tests are disabled")
endif ()

# Add target for common lib, which contains a minimal set
add_subdirectory(grpc)
# of shared functionality used by all components of nes
add_subdirectory(nes-common)
# Add target for all data types
add_subdirectory(nes-data-types)
# Add target for the runtime
add_subdirectory(nes-runtime)
# Add target for configurations
add_subdirectory(nes-configurations)
# Add target for operators
add_subdirectory(nes-operators)
# Add target for client
add_subdirectory(nes-client)
# Add targets for execution
add_subdirectory(nes-execution)
# Add targets for nautilus
add_subdirectory(nes-nautilus)
# Add targets nes-expressions
add_subdirectory(nes-expressions)
# Add targets nes-window-types
add_subdirectory(nes-window-types)

add_subdirectory(nes-single-node-worker)

# Documentation
add_subdirectory(cmake/docs)

# Custom test commands --------------------------------------------------------
# The test_default target runs tests without any special suffix
message(STATUS "Adding test_debug target with parallelism=${NES_TEST_PARALLELISM}")
set(NES_TEST_REPEAT_UNTIL_FAIL 3)
set(NES_TEST_TIMEOUT 300)
add_custom_target(test_default COMMAND ${CMAKE_CTEST_COMMAND} --parallel ${NES_TEST_PARALLELISM} --test-load 90 --timeout ${NES_TEST_TIMEOUT} --repeat until-pass:${NES_TEST_REPEAT_UNTIL_FAIL} --output-on-failure DEPENDS ${UNITTESTS})

#Other configurations
project_enable_clang_format()
project_enable_check_comment_format()
project_enable_check_preamble()
instrument_codebase()
