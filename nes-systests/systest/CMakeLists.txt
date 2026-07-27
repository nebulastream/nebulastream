# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(src)
add_subdirectory(checksum)

# Include necessary modules
include(${CMAKE_SOURCE_DIR}/cmake/CodeCoverage.cmake)

# This uses ExternalData to manage test data for the system tests.
# Files can either be stored directly in the testdata/small folder, but this is only permitted for small files or
# referenced via a hash an fetched from a remote server on demand. Large files have to be uploaded to bench.nebula.stream/data (ask a maintainer)!
include(ExternalData)
set(ExternalData_URL_TEMPLATES
        "https://tubcloud.tu-berlin.de/s/28Tr2wTd73Ggeed/download?files=%(algo)_%(hash)"
)
if (ENABLE_LARGE_TESTS)
    set(TEST_DATA_PATH DATA{../testdata/,RECURSE:,REGEX:.*})
else ()
    # By default this only includes small files located in testdata/small
    set(TEST_DATA_PATH DATA{../testdata/,RECURSE:,REGEX:small.*})
endif ()
set(ExternalData_TIMEOUT_ABSOLUTE ${LARGE_TEST_DOWNLOAD_TIMEOUT})
ExternalData_Expand_Arguments(
        test-data
        EXPANDED_TEST_DATA_PATH
        ${TEST_DATA_PATH}
)
message(STATUS "Systest testdata is located at ${EXPANDED_TEST_DATA_PATH}")

get_source(nes-systest-lib SLT_SOURCE_FILES)

find_package(reflectcpp CONFIG REQUIRED)

add_library(nes-systest-lib STATIC ${SLT_SOURCE_FILES})
target_link_libraries(nes-systest-lib PUBLIC nes-frontend-embedded-lib nes-single-node-worker-lib nes-sql-parser nes-query-optimizer reflectcpp::reflectcpp PRIVATE nes-input-formatters)
target_include_directories(nes-systest-lib PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_compile_definitions(nes-systest-lib PUBLIC
        PATH_TO_BINARY_DIR="${CMAKE_BINARY_DIR}"
        TEST_DISCOVER_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../"
        SYSTEST_EXTERNAL_DATA_DIR="${EXPANDED_TEST_DATA_PATH}"
        TEST_CONFIGURATION_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../configs"
)

# Find required packages
find_package(argparse CONFIG REQUIRED)

# Add the systest executable
add_executable(systest src/SystestStarter.cpp)
set_property(TARGET systest PROPERTY ENABLE_EXPORTS ON)
target_link_libraries(systest PUBLIC nes-systest-lib argparse::argparse nes-query-optimizer)
target_include_directories(systest PUBLIC $<INSTALL_INTERFACE:/include/nebulastream>)

# By default, only run with slice cache enabled. When ALL_SLICE_CACHE_TESTS is ON, also run without.
if (ALL_SLICE_CACHE_TESTS)
    set(sliceCacheValues false true)
else ()
    set(sliceCacheValues true)
endif ()

# Group exclude lists for systest invocations. The Inference group requires the IREE toolchain;
# when ENABLE_IREE_TESTS is OFF (e.g. iree-import-onnx / iree-compile not in PATH) we exclude it
# so the .test files in nes-systests/inference/ are skipped at runtime.
set(SYSTEST_EXCLUDE_GROUPS large)
set(SYSTEST_EXCLUDE_GROUPS_COMPILER large CompilationIntensive)
if (NOT ENABLE_IREE_TESTS)
    list(APPEND SYSTEST_EXCLUDE_GROUPS Inference)
    list(APPEND SYSTEST_EXCLUDE_GROUPS_COMPILER Inference)
endif ()
# The NestedLoopIntensive group holds tests whose input is large enough that the O(n^2) nested loop join takes over an
# hour per query, in the compiler just as much as in the interpreter. So exclude them from both NLJ invocations.
# They still run for every other join strategy.
set(SYSTEST_EXCLUDE_GROUPS_NLJ ${SYSTEST_EXCLUDE_GROUPS} NestedLoopIntensive)
set(SYSTEST_EXCLUDE_GROUPS_NLJ_COMPILER ${SYSTEST_EXCLUDE_GROUPS_COMPILER} NestedLoopIntensive)

# Memory budget for every systest invocation that runs the join tests. The outer-join tests hold far more unpooled state
# than the rest of the suite: they keep one ChainedHashMap per build side per worker thread alive for every open slice,
# and a 1 s tumbling window over the 10,000-row outer-join inputs opens a lot of slices at once. That overruns the
# 128 MiB unpooled share the default budget leaves. Only the unpooled share is short, so grow just that and keep the
# pooled share at the default 128 MiB (32,768 operator buffers), i.e. no extra memory is reserved up front.
set(SYSTEST_JOIN_TOTAL_MEMORY_BYTES 805306368) # 128 MiB pooled (1/6) + 640 MiB unpooled (5/6)
set(SYSTEST_JOIN_UNPOOLED_FRACTION 0.8333333)

# Add code coverage if enabled
if (CODE_COVERAGE)
    foreach (enableSliceCache IN LISTS sliceCacheValues)
        target_code_coverage(systest COVERAGE_TARGET_NAME systest_interpreter_coverage_sliceCache_${enableSliceCache} PUBLIC AUTO ALL ARGS -n 20 --show-query-performance --exclude-groups ${SYSTEST_EXCLUDE_GROUPS_NLJ} --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_cc_sliceCache_${enableSliceCache} --data ${EXPANDED_TEST_DATA_PATH} --optimizer join_strategy=NESTED_LOOP_JOIN -- --worker.default_query_execution.execution_mode=INTERPRETER --enable_event_trace=true --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
        target_code_coverage(systest COVERAGE_TARGET_NAME systest_interpreter_coverage_Hash_Join_sliceCache_${enableSliceCache} PUBLIC AUTO ALL ARGS -n 20 --show-query-performance --exclude-groups ${SYSTEST_EXCLUDE_GROUPS} --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_cc_sliceCache_${enableSliceCache} --data ${EXPANDED_TEST_DATA_PATH} --optimizer join_strategy=HASH_JOIN -- --worker.default_query_execution.execution_mode=INTERPRETER --enable_event_trace=true --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
        # Make sure to fetch ExternalData before running the ccov targets
        add_dependencies(ccov-run-systest_interpreter_coverage_sliceCache_${enableSliceCache} test-data)
        add_dependencies(ccov-run-systest_interpreter_coverage_Hash_Join_sliceCache_${enableSliceCache} test-data)
    endforeach ()
endif (CODE_COVERAGE)

# If we are running code coverage, we need to ONLY run the interpreter tests, as otherwise, the code coverage will be 100% for all operators as the compiler traces all branches and operations.
set(joinStrategies NESTED_LOOP_JOIN HASH_JOIN)
# The systest_interpreter and _compiler pass quite quickly, thus, we run them w/ and w/o a slice cache (if ALL_SLICE_CACHE_TESTS is ON)
foreach (enableSliceCache IN LISTS sliceCacheValues)
    foreach (joinStrategy IN LISTS joinStrategies)
        if (joinStrategy STREQUAL NESTED_LOOP_JOIN)
            set(interpreterExcludeGroups ${SYSTEST_EXCLUDE_GROUPS_NLJ})
        else ()
            set(interpreterExcludeGroups ${SYSTEST_EXCLUDE_GROUPS})
        endif ()
        ExternalData_Add_Test(test-data
                NAME systest_interpreter_${joinStrategy}_sliceCache_${enableSliceCache}
                COMMAND systest -n 20 --show-query-performance --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_${joinStrategy}_sliceCache_${enableSliceCache} --exclude-groups ${interpreterExcludeGroups} --data ${EXPANDED_TEST_DATA_PATH} --optimizer join_strategy=${joinStrategy} -- --worker.query_engine.number_of_worker_threads=4 --worker.default_query_execution.execution_mode=INTERPRETER --worker.total_memory_in_bytes=${SYSTEST_JOIN_TOTAL_MEMORY_BYTES} --worker.unpooled_memory_fraction=${SYSTEST_JOIN_UNPOOLED_FRACTION} --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
        set_tests_properties(systest_interpreter_${joinStrategy}_sliceCache_${enableSliceCache} PROPERTIES LABELS "Systest" PROCESSORS 4)
    endforeach ()
endforeach ()
if (NOT CODE_COVERAGE)
    foreach (enableSliceCache IN LISTS sliceCacheValues)
        ExternalData_Add_Test(test-data
                NAME systest_compiler_sliceCache_${enableSliceCache}
                COMMAND systest -n 20 --show-query-performance --workingDir=${CMAKE_CURRENT_BINARY_DIR}/compiler_sliceCache_${enableSliceCache} --exclude-groups ${SYSTEST_EXCLUDE_GROUPS_COMPILER} --data ${EXPANDED_TEST_DATA_PATH} -- --worker.query_engine.number_of_worker_threads=4 --worker.default_query_execution.execution_mode=COMPILER --enable_event_trace=true --worker.total_memory_in_bytes=${SYSTEST_JOIN_TOTAL_MEMORY_BYTES} --worker.unpooled_memory_fraction=${SYSTEST_JOIN_UNPOOLED_FRACTION} --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
        set_tests_properties(systest_compiler_sliceCache_${enableSliceCache} PROPERTIES LABELS "Systest" PROCESSORS 4)
    endforeach ()
endif (NOT CODE_COVERAGE)


if (ENABLE_LARGE_TESTS)
    message(STATUS "Large scale tests enabled")

    # Memory budget for the in-process large systests, split into a pooled and an unpooled share.
    # The unpooled pool (hash maps, paged vectors, var-sized data) is bounded so a runaway query fails cleanly instead of
    # OOM-killing the worker. Sized so the pooled share keeps ~2,000,000 operator buffers (the previous setting) while
    # leaving 64 GiB for unpooled state; raise it if a future large test legitimately needs more memory.
    set(LARGE_TEST_TOTAL_MEMORY_BYTES 76911476736 CACHE STRING
            "Memory budget (bytes) for the in-process large systests: 8 GiB pooled (2,000,000 * 4096) + 64 GiB unpooled")
    set(LARGE_TEST_UNPOOLED_FRACTION 0.8935) # 68719476736 / 76911476736

    set(joinStrategy HASH_JOIN) # We simply test here with a hash join, as a NLJ does not finish in 2h
    ExternalData_Add_Test(test-data
            # currently, 4 worker threads perform the best on the large systests (avoids lock contention on NEXMARK Query 5)
            NAME systest_large_${joinStrategy}
            COMMAND systest -n 6 --show-query-performance --workingDir=${CMAKE_CURRENT_BINARY_DIR}/large_scale_${joinStrategy} --data ${EXPANDED_TEST_DATA_PATH} --groups large --optimizer join_strategy=${joinStrategy}  -- --worker.default_query_execution.execution_mode=COMPILER --worker.query_engine.number_of_worker_threads=4 --worker.total_memory_in_bytes=${LARGE_TEST_TOTAL_MEMORY_BYTES} --worker.unpooled_memory_fraction=${LARGE_TEST_UNPOOLED_FRACTION} --worker.default_query_execution.slice_cache.enable_slice_cache=true)
    set_tests_properties(systest_large_${joinStrategy} PROPERTIES LABELS "Systest" PROCESSORS 4)

    ExternalData_Add_Test(test-data
            # currently, 16 worker threads perform the best on the large systests (avoids lock contention on NEXMARK Query 5)
            NAME systest_large_benchmark_${joinStrategy}
            COMMAND systest -b --show-query-performance --workingDir=${CMAKE_CURRENT_BINARY_DIR}/large_scale_benchmark_${joinStrategy} --data ${EXPANDED_TEST_DATA_PATH} --groups large --optimizer join_strategy=${joinStrategy}  -- --worker.default_query_execution.execution_mode=COMPILER --worker.query_engine.number_of_worker_threads=16 --worker.total_memory_in_bytes=${LARGE_TEST_TOTAL_MEMORY_BYTES} --worker.unpooled_memory_fraction=${LARGE_TEST_UNPOOLED_FRACTION} --worker.default_query_execution.slice_cache.enable_slice_cache=true)
    set_tests_properties(systest_large_benchmark_${joinStrategy} PROPERTIES LABELS "Systest" PROCESSORS 16)

    # Running here the Nexmark.test queries with NLJ to test it for larger data sets but the input data size
    # is still small enough to finish the test in minutes.
    set(joinStrategy NESTED_LOOP_JOIN)
    ExternalData_Add_Test(TEST_DATA_LARGE
            NAME systest_large_${joinStrategy}
            COMMAND systest -n 6 --show-query-performance --workingDir=${CMAKE_CURRENT_BINARY_DIR}/large_scale_${joinStrategy} --data ${EXPANDED_TEST_DATA_PATH} -t ${CMAKE_CURRENT_SOURCE_DIR}/../benchmark/Nexmark.test:05 --optimizer join_strategy=${joinStrategy} -- --worker.default_query_execution.execution_mode=COMPILER --worker.query_engine.number_of_worker_threads=4 --worker.total_memory_in_bytes=16384000000 --worker.default_query_execution.slice_cache.enable_slice_cache=true)
    set_tests_properties(systest_large_${joinStrategy} PROPERTIES LABELS "Systest" PROCESSORS 4)

    # The in-process large tests above each run one worker at LARGE_TEST_TOTAL_MEMORY_BYTES on this host. Fail loudly at
    # configure time if the host cannot back a single worker's budget rather than silently dropping the coverage or
    # OOM-failing the test at runtime. The distributed suite runs several workers per host; distributed.bats checks that
    # aggregate against the budgets declared in its topology YAML.
    cmake_host_system_information(RESULT _host_physical_memory_mib QUERY TOTAL_PHYSICAL_MEMORY)
    math(EXPR _large_test_required_mib "${LARGE_TEST_TOTAL_MEMORY_BYTES} / 1048576")
    if (_host_physical_memory_mib LESS _large_test_required_mib)
        message(FATAL_ERROR
                "Large systests require ${_large_test_required_mib} MiB of physical memory per worker "
                "(total_memory_in_bytes budget) but this host has only ${_host_physical_memory_mib} MiB. Either lower "
                "the budget with -DLARGE_TEST_TOTAL_MEMORY_BYTES=<bytes>, or turn off the large suite with "
                "-DENABLE_LARGE_TESTS=OFF.")
    endif ()
endif ()


if (NOT CODE_COVERAGE)
    ## We run all join and aggregation tests with different no. worker threads, different join strategies, and with and w/o slice cache (if ALL_SLICE_CACHE_TESTS is ON)
    foreach (enableSliceCache IN LISTS sliceCacheValues)
        foreach (workerThreads IN ITEMS 1 2 4)
            foreach (joinStrategy IN ITEMS NESTED_LOOP_JOIN HASH_JOIN)
                if (joinStrategy STREQUAL NESTED_LOOP_JOIN)
                    set(interpreterExcludeGroups ${SYSTEST_EXCLUDE_GROUPS_NLJ})
                    set(compilerExcludeGroups ${SYSTEST_EXCLUDE_GROUPS_NLJ_COMPILER})
                else ()
                    set(interpreterExcludeGroups ${SYSTEST_EXCLUDE_GROUPS})
                    set(compilerExcludeGroups ${SYSTEST_EXCLUDE_GROUPS_COMPILER})
                endif ()
                ExternalData_Add_Test(test-data
                        NAME systest_join_${workerThreads}_${joinStrategy}_sliceCache_${enableSliceCache}_interpreter
                        COMMAND systest -n 6 --show-query-performance --groups Join --exclude-groups ${interpreterExcludeGroups} --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${joinStrategy}_sliceCache_${enableSliceCache}_interpreter_join --data ${EXPANDED_TEST_DATA_PATH}
                        --optimizer join_strategy=${joinStrategy}
                        --
                        --worker.query_engine.number_of_worker_threads=${workerThreads} --worker.default_query_execution.execution_mode=INTERPRETER --worker.total_memory_in_bytes=${SYSTEST_JOIN_TOTAL_MEMORY_BYTES} --worker.unpooled_memory_fraction=${SYSTEST_JOIN_UNPOOLED_FRACTION} --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
                set_tests_properties(systest_join_${workerThreads}_${joinStrategy}_sliceCache_${enableSliceCache}_interpreter PROPERTIES LABELS "Systest" PROCESSORS ${workerThreads})
                ExternalData_Add_Test(test-data
                        NAME systest_join_${workerThreads}_${joinStrategy}_sliceCache_${enableSliceCache}_compiler
                        COMMAND systest -n 6 --show-query-performance --groups Join --exclude-groups ${compilerExcludeGroups} --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${joinStrategy}_sliceCache_${enableSliceCache}_compiler_join --data ${EXPANDED_TEST_DATA_PATH}
                        --optimizer join_strategy=${joinStrategy}
                        --
                        --worker.query_engine.number_of_worker_threads=${workerThreads} --worker.default_query_execution.execution_mode=COMPILER --worker.total_memory_in_bytes=${SYSTEST_JOIN_TOTAL_MEMORY_BYTES} --worker.unpooled_memory_fraction=${SYSTEST_JOIN_UNPOOLED_FRACTION} --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
                set_tests_properties(systest_join_${workerThreads}_${joinStrategy}_sliceCache_${enableSliceCache}_compiler PROPERTIES LABELS "Systest" PROCESSORS ${workerThreads})
            endforeach ()
            ExternalData_Add_Test(test-data
                    NAME systest_agg_${workerThreads}_sliceCache_${enableSliceCache}_interpreter
                    COMMAND systest -n 6 --show-query-performance --groups Aggregation --exclude-groups ${SYSTEST_EXCLUDE_GROUPS} --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_sliceCache_${enableSliceCache}_interpreter_aggregation --data ${EXPANDED_TEST_DATA_PATH}
                    --
                    --worker.query_engine.number_of_worker_threads=${workerThreads} --worker.default_query_execution.execution_mode=INTERPRETER --worker.total_memory_in_bytes=163840000 --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
            set_tests_properties(systest_agg_${workerThreads}_sliceCache_${enableSliceCache}_interpreter PROPERTIES LABELS "Systest" PROCESSORS ${workerThreads})
            ExternalData_Add_Test(test-data
                    NAME systest_agg_${workerThreads}_sliceCache_${enableSliceCache}_compiler
                    COMMAND systest -n 6 --show-query-performance --groups Aggregation --exclude-groups ${SYSTEST_EXCLUDE_GROUPS_COMPILER} --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_sliceCache_${enableSliceCache}_compiler_aggregation --data ${EXPANDED_TEST_DATA_PATH}
                    --
                    --worker.query_engine.number_of_worker_threads=${workerThreads} --worker.default_query_execution.execution_mode=COMPILER --worker.total_memory_in_bytes=163840000 --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
            set_tests_properties(systest_agg_${workerThreads}_sliceCache_${enableSliceCache}_compiler PROPERTIES LABELS "Systest" PROCESSORS ${workerThreads})
        endforeach ()
    endforeach ()
endif (NOT CODE_COVERAGE)

# Adding dependency for the nes-systest-lib so that the data is downloaded before the tests are run
ExternalData_Add_Target(test-data)
add_dependencies(nes-systest-lib test-data)

add_tests_if_enabled(tests)

# Per-worker memory budgets for the distributed suite live in the topology YAMLs themselves (the large-scale
# two-node topology declares one; the lightweight topologies keep the worker default). distributed.bats sums the
# declared budgets and checks the host can back them before starting the compose stack.
add_e2e_test(
    NAME systest-remote-test
    BATS_FILE remote-test/distributed.bats
    DOCKER_COMPOSE
    EXTRA_ENV
        DATADIR=${EXPANDED_TEST_DATA_PATH}
        ENABLE_LARGE_TESTS=${ENABLE_LARGE_TESTS}
)
