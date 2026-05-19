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
option(ENABLE_LARGE_TESTS "Runs testcases with larger input data" OFF)
option(ENABLE_DOCKER_TESTS "Runs testcases that require docker" ON)

option(NES_ENABLE_PRECOMPILED_HEADERS "Enable precompiled headers (might improve compilation time)" OFF)
option(NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR "Enables the MLIR backend." ON)
option(NES_LOG_WITH_STACKTRACE "Log exceptions with stacktrace" ON)
option(NES_DEBUG_TUPLE_BUFFER_LEAKS "Heavyweight instrumentation for Tuplebuffer debugging" OFF)
option(ALL_HASHMAP_TESTS "Run all hashmap test combinations." OFF)
option(ALL_SLICE_CACHE_TESTS "Run system tests with and without slice cache." OFF)


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


set(CMAKE_THREAD_LIBS_INIT "-lpthread")
include(cmake/HostIdentification.cmake)
include(cmake/Sanitizers.cmake)

if (NES_LOG_WITH_STACKTRACE)
    if (SANITIZER_OPTION STREQUAL "thread")
        message(WARNING "NES_LOG_WITH_STACKTRACE is ON but is disabled because the thread sanitizer is active. "
                "Stack trace resolution is prohibitively slow under TSAN and causes test timeouts.")
    else ()
        add_compile_definitions(NES_LOG_WITH_STACKTRACE)
    endif ()
endif ()
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
if (NES_DEBUG_TUPLE_BUFFER_LEAKS)
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
set(NES_WARNINGS "-Wall -Wconstant-conversion -Wbitwise-instead-of-logical -Wextra -pedantic -Wno-null-character -Wno-dollar-in-identifier-extension -Werror=extra -Werror=exceptions -Werror=all -Werror=integer-overflow -Werror=return-type -Werror=return-stack-address -Werror=delete-non-virtual-dtor -Werror=deprecated -Werror=writable-strings -Werror=array-bounds -Werror=ignored-qualifiers -Werror=sign-compare -Wno-deprecated-copy-with-dtor -Wno-unused-variable -Wno-unused-but-set-variable -Wno-deprecated-declarations -Wno-invalid-offsetof -Wno-nullability-extension")
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
include(cmake/UseCompilerCache.cmake)
include(cmake/UseMold.cmake)
include(cmake/EnableRust.cmake)
include(cmake/CheckTestUtilities.cmake)

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
    if (NES_ARCH STREQUAL "x64")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=x86-64 -mtune=generic")
    elseif (NES_ARCH STREQUAL "arm64")
        # Arm themselves suggest using -mcpu=native, or in general,
        # to use -mcpu=CPU_TYPE. For more info, here:
        # https://community.arm.com/developer/tools-software/tools/b/tools-software-ides-blog/posts/compiler-flags-across-architectures-march-mtune-and-mcpu
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=armv8-a")
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
    if (ENABLE_DOCKER_TESTS)
        set(NES_RUNTIME_BASE_IMAGE "nes-runtime-base:test" CACHE STRING "Docker image name for the runtime base used by DockerCompose tests")
        add_test(NAME build-runtime-base-image
                COMMAND docker build --load -t ${NES_RUNTIME_BASE_IMAGE}
                -f ${CMAKE_SOURCE_DIR}/docker/runtime/RuntimeBase.dockerfile
                ${CMAKE_SOURCE_DIR}/docker/runtime
        )
        set_tests_properties(build-runtime-base-image PROPERTIES
                FIXTURES_SETUP RuntimeBaseImage
                LABELS "DockerCompose"
        )
    endif ()
    if (CODE_COVERAGE)
        include(cmake/CodeCoverage.cmake)
        add_code_coverage_all_targets(EXCLUDE ${CMAKE_BINARY_DIR}/* tests/*)
        message(STATUS "Tests are enabled with code coverage")

        # FIXTURES_CLEANUP test that ctest auto-schedules after every bats
        # test (each is FIXTURES_REQUIRED BatsCoverage — see cmake/macros.cmake).
        # Copies the *.profraw files bats binaries wrote under build/test-tmp
        # into a dedicated build/ccov/bats-profraws/ that ccov-run-bats owns.
        # `cp --parents` preserves the source subpath, so deterministic per-test
        # TMP_DIRs (distributed_bats_lib.bash) → deterministic destination
        # filenames here → cmake-codecov's profraw.list entries stay valid
        # across reruns even though paths are appended each time.
        # build/test-tmp itself is left alone so developers can inspect each
        # test's working dir post-mortem.
        set(BATS_COVERAGE_DIR ${CMAKE_BINARY_DIR}/ccov/bats-profraws)
        add_test(NAME bats-coverage-collect
                COMMAND bash -c "mkdir -p '${BATS_COVERAGE_DIR}' && if [ -d '${CMAKE_BINARY_DIR}/test-tmp' ]; then cd '${CMAKE_BINARY_DIR}/test-tmp' && find . -type f -name '*.profraw' -size +0c -exec cp --parents {} '${BATS_COVERAGE_DIR}/' \\;; fi")
        set_tests_properties(bats-coverage-collect PROPERTIES
                FIXTURES_CLEANUP BatsCoverage
                LABELS Bats)
    endif ()
else ()
    message(STATUS "Tests are disabled")
endif ()

add_custom_target(build_all_plugins)

# Adds sub-directories
file(GLOB NES_DIRECTORIES_ALL RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "nes-*")
set(NES_DIRECTORIES)
foreach (NES_DIR ${NES_DIRECTORIES_ALL})
    if (IS_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/${NES_DIR} AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${NES_DIR}/CMakeLists.txt)
        list(APPEND NES_DIRECTORIES ${NES_DIR})
        add_subdirectory(${NES_DIR})
    endif ()
endforeach ()

# Other configurations
project_enable_format()

# Adding targets for code coverage if it is enabled
if (CODE_COVERAGE)
    set(NES_COVERAGE_DIRECTORIES ${NES_DIRECTORIES})
    list(REMOVE_ITEM NES_COVERAGE_DIRECTORIES "nes-systests")
    list(REMOVE_ITEM NES_COVERAGE_DIRECTORIES "nes-frontend")
    list(REMOVE_ITEM NES_COVERAGE_DIRECTORIES "nes-single-node-worker")
    # 'nes-optional-plugins' is not a target on its own, the plugins are part of other targets
    list(REMOVE_ITEM NES_COVERAGE_DIRECTORIES "nes-plugins")
    list(REMOVE_ITEM NES_COVERAGE_DIRECTORIES "nes-network")
    foreach (NES_DIR ${NES_COVERAGE_DIRECTORIES})
        target_code_coverage(${NES_DIR} PUBLIC AUTO ALL)
    endforeach ()
    # Libraries the e2e binaries (nes-cli, nes-repl, nes-repl-embedded,
    # nes-single-node-worker, systest) link against. PUBLIC propagates
    # -fprofile-instr-generate/-fcoverage-mapping (compile + link) to the
    # executables via INTERFACE_*, so the exes' own *Starter.cpp and friends
    # also get instrumented without needing per-exe target_code_coverage calls.
    target_code_coverage(nes-single-node-worker-lib PUBLIC AUTO ALL)
    target_code_coverage(nes-single-node-worker-interface PUBLIC AUTO ALL)
    target_code_coverage(nes-frontend-lib PUBLIC AUTO ALL)
    target_code_coverage(nes-frontend-embedded-lib PUBLIC AUTO ALL)
    target_code_coverage(nes-systest-lib PUBLIC AUTO ALL)

    # Plug the bats-driven e2e tests into cmake-codecov's aggregator the same
    # way per-target gtest runners do: a `ccov-run-bats` target invokes the
    # tests (via ctest -L Bats), then appends the produced profraws and the
    # e2e binaries to profraw.list / binaries.list. ccov-all-processing depends
    # on it and merges everything into all-merged.profdata, so the existing
    # ccov-all-export / ccov-all (HTML) targets cover gtest + bats from a
    # single invocation:
    #
    #   cmake --build <dir> --target ccov-all-export
    #
    # Bats tests write *.profraw into cwd (LLVM_PROFILE_FILE set by
    # add_e2e_test). For offline tests that's the host TMP_DIR; for
    # docker-compose tests it's /workdir inside the container, synced back to
    # the host TMP_DIR by sync_workdir in distributed_bats_lib.bash. Either
    # way, everything lands under ${CMAKE_BINARY_DIR}/test-tmp.
    set(_ccov_bats_binaries
        nes-single-node-worker
        nes-cli
        nes-repl
        nes-repl-embedded
        systest)
    # One -E echo per binary, each its own shell invocation so the >> redirect
    # applies per binary (same pattern cmake-codecov uses in
    # CodeCoverage.cmake:319 for ccov-run-<gtest>).
    set(_ccov_bats_append_objects)
    foreach (_bin IN LISTS _ccov_bats_binaries)
        list(APPEND _ccov_bats_append_objects
            COMMAND ${CMAKE_COMMAND} -E echo "-object=$<TARGET_FILE:${_bin}>"
                >> ${CMAKE_COVERAGE_OUTPUT_DIRECTORY}/binaries.list)
    endforeach ()
    # Same five-stage structure as ccov-run-<gtest> (CodeCoverage.cmake:312):
    # wipe → run-something → find/echo paths into profraw.list → echo
    # binaries into binaries.list. The "run something" is `ctest -L Bats`,
    # which transitively also runs the bats-coverage-collect FIXTURES_CLEANUP
    # test that copies profraws from build/test-tmp into BATS_COVERAGE_DIR.
    # Deterministic test-tmp paths (distributed_bats_lib.bash) give us stable
    # destination filenames, so profraw.list entries stay valid across reruns
    # even though they're appended each time (mirrors gtest's <name>.profraw).
    add_custom_target(ccov-run-bats
            COMMAND ${CMAKE_COMMAND} -E rm -rf ${BATS_COVERAGE_DIR}
            COMMAND ${CMAKE_COMMAND} -E make_directory ${BATS_COVERAGE_DIR}
            COMMAND ${CMAKE_CTEST_COMMAND} --test-dir ${CMAKE_BINARY_DIR} -L Bats --output-on-failure
            COMMAND bash -c "find ${BATS_COVERAGE_DIR} -type f -name '*.profraw' ! -size 0 >> ${CMAKE_COVERAGE_OUTPUT_DIRECTORY}/profraw.list"
            ${_ccov_bats_append_objects}
            DEPENDS ccov-preprocessing ${_ccov_bats_binaries}
            COMMENT "Running bats e2e tests for coverage")
    add_dependencies(ccov-all-processing ccov-run-bats)
endif ()
