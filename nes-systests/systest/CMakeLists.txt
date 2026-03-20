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
set(ExternalData_TIMEOUT_ABSOLUTE 1200)
ExternalData_Expand_Arguments(
        test-data
        EXPANDED_TEST_DATA_PATH
        ${TEST_DATA_PATH}
)
message(STATUS "Systest testdata is located at ${EXPANDED_TEST_DATA_PATH}")

get_source(nes-systest-lib SLT_SOURCE_FILES)

add_library(nes-systest-lib STATIC ${SLT_SOURCE_FILES})
target_link_libraries(nes-systest-lib PUBLIC nes-frontend-embedded-lib nes-single-node-worker-lib nes-sql-parser nes-query-optimizer PRIVATE nes-input-formatters)
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

# Add code coverage if enabled
if (CODE_COVERAGE)
    foreach (enableSliceCache IN ITEMS false true)
        target_code_coverage(systest COVERAGE_TARGET_NAME systest_interpreter_coverage_sliceCache_${enableSliceCache} PUBLIC AUTO ALL ARGS -n 20 --show-query-performance --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_cc_sliceCache_${enableSliceCache} --data ${EXPANDED_TEST_DATA_PATH} --optimizer join_strategy=NESTED_LOOP_JOIN -- --worker.default_query_execution.execution_mode=INTERPRETER --enable_event_trace=true --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
        target_code_coverage(systest COVERAGE_TARGET_NAME systest_interpreter_coverage_Hash_Join_sliceCache_${enableSliceCache} PUBLIC AUTO ALL ARGS -n 20 --show-query-performance --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_cc_sliceCache_${enableSliceCache} --data ${EXPANDED_TEST_DATA_PATH} --optimizer join_strategy=HASH_JOIN -- --worker.default_query_execution.execution_mode=INTERPRETER --enable_event_trace=true --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
        # Make sure to fetch ExternalData before running the ccov targets
        add_dependencies(ccov-run-systest_interpreter_coverage_sliceCache_${enableSliceCache} test-data)
        add_dependencies(ccov-run-systest_interpreter_coverage_Hash_Join_sliceCache_${enableSliceCache} test-data)
    endforeach ()
endif (CODE_COVERAGE)

# If we are running code coverage, we need to ONLY run the interpreter tests, as otherwise, the code coverage will be 100% for all operators as the compiler traces all branches and operations.
set(joinStrategies NESTED_LOOP_JOIN HASH_JOIN)
# The systest_interpreter and _compiler pass quite quickly, thus, we run them w/ and w/o a slice cache
foreach (enableSliceCache IN ITEMS false true)
    foreach (joinStrategy IN LISTS joinStrategies)
        ExternalData_Add_Test(test-data
                NAME systest_interpreter_${joinStrategy}_sliceCache_${enableSliceCache}
                COMMAND systest -n 20 --show-query-performance --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_${joinStrategy}_sliceCache_${enableSliceCache} --exclude-groups large --data ${EXPANDED_TEST_DATA_PATH} --optimizer join_strategy=${joinStrategy} -- --worker.query_engine.number_of_worker_threads=4 --worker.default_query_execution.execution_mode=INTERPRETER --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
        set_tests_properties(systest_interpreter_${joinStrategy}_sliceCache_${enableSliceCache} PROPERTIES LABELS "Systest" PROCESSORS 4)
    endforeach ()
endforeach ()
if (NOT CODE_COVERAGE)
    foreach (enableSliceCache IN ITEMS false true)
        ExternalData_Add_Test(test-data
                NAME systest_compiler_sliceCache_${enableSliceCache}
                COMMAND systest -n 20 --show-query-performance --workingDir=${CMAKE_CURRENT_BINARY_DIR}/compiler_sliceCache_${enableSliceCache} --exclude-groups large CompilationIntensive --data ${EXPANDED_TEST_DATA_PATH} -- --worker.query_engine.number_of_worker_threads=4 --worker.default_query_execution.execution_mode=COMPILER --enable_event_trace=true --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
        set_tests_properties(systest_compiler_sliceCache_${enableSliceCache} PROPERTIES LABELS "Systest" PROCESSORS 4)
    endforeach ()
endif (NOT CODE_COVERAGE)


if (ENABLE_LARGE_TESTS)
    message(STATUS "Large scale tests enabled")

    set(joinStrategy HASH_JOIN) # We simply test here with a hash join, as a NLJ does not finish in 2h
    ExternalData_Add_Test(test-data
            # currently, 4 worker threads perform the best on the large systests (avoids lock contention on NEXMARK Query 5)
            NAME systest_large_${joinStrategy}
            COMMAND systest -n 6 --show-query-performance --workingDir=${CMAKE_CURRENT_BINARY_DIR}/large_scale_${joinStrategy} --data ${EXPANDED_TEST_DATA_PATH} --groups large --optimizer join_strategy=${joinStrategy}  -- --worker.default_query_execution.execution_mode=COMPILER --worker.query_engine.number_of_worker_threads=4 --worker.number_of_buffers_in_global_buffer_manager=2000000 --worker.default_query_execution.slice_cache.enable_slice_cache=true)
    set_tests_properties(systest_large_${joinStrategy} PROPERTIES LABELS "Systest" PROCESSORS 4)

    ExternalData_Add_Test(test-data
            # currently, 16 worker threads perform the best on the large systests (avoids lock contention on NEXMARK Query 5)
            NAME systest_large_benchmark_${joinStrategy}
            COMMAND systest -b --show-query-performance --workingDir=${CMAKE_CURRENT_BINARY_DIR}/large_scale_benchmark_${joinStrategy} --data ${EXPANDED_TEST_DATA_PATH} --groups large --optimizer join_strategy=${joinStrategy}  -- --worker.default_query_execution.execution_mode=COMPILER --worker.query_engine.number_of_worker_threads=16 --worker.number_of_buffers_in_global_buffer_manager=2000000 --worker.default_query_execution.slice_cache.enable_slice_cache=true)
    set_tests_properties(systest_large_benchmark_${joinStrategy} PROPERTIES LABELS "Systest" PROCESSORS 16)

    # Running here the Nexmark.test queries with NLJ to test it for larger data sets but the input data size
    # is still small enough to finish the test in minutes.
    set(joinStrategy NESTED_LOOP_JOIN)
    ExternalData_Add_Test(TEST_DATA_LARGE
            NAME systest_large_${joinStrategy}
            COMMAND systest -n 6 --show-query-performance --workingDir=${CMAKE_CURRENT_BINARY_DIR}/large_scale_${joinStrategy} --data ${EXPANDED_TEST_DATA_PATH} -t ${CMAKE_CURRENT_SOURCE_DIR}/../benchmark/Nexmark.test:05 --optimizer join_strategy=${joinStrategy} -- --worker.default_query_execution.execution_mode=COMPILER --worker.query_engine.number_of_worker_threads=4 --worker.number_of_buffers_in_global_buffer_manager=2000000 --worker.default_query_execution.slice_cache.enable_slice_cache=true})
    set_tests_properties(systest_large_${joinStrategy} PROPERTIES LABELS "Systest" PROCESSORS 4)
endif ()


if (NOT CODE_COVERAGE)
    ## We run all join and aggregation tests with different no. worker threads, different join strategies, and with and w/o slice cache
    foreach (enableSliceCache IN ITEMS false true)
        foreach (workerThreads IN ITEMS 1 2 4)
            foreach (joinStrategy IN ITEMS NESTED_LOOP_JOIN HASH_JOIN)
                ExternalData_Add_Test(test-data
                        NAME systest_join_${workerThreads}_${joinStrategy}_sliceCache_${enableSliceCache}_interpreter
                        COMMAND systest -n 6 --show-query-performance --groups Join --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${joinStrategy}_sliceCache_${enableSliceCache}_interpreter_join --data ${EXPANDED_TEST_DATA_PATH}
                        --optimizer join_strategy=${joinStrategy}
                        --
                        --worker.query_engine.number_of_worker_threads=${workerThreads} --worker.default_query_execution.execution_mode=INTERPRETER --worker.number_of_buffers_in_global_buffer_manager=20000 --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
                set_tests_properties(systest_join_${workerThreads}_${joinStrategy}_sliceCache_${enableSliceCache}_interpreter PROPERTIES LABELS "Systest" PROCESSORS ${workerThreads})
                ExternalData_Add_Test(test-data
                        NAME systest_join_${workerThreads}_${joinStrategy}_sliceCache_${enableSliceCache}_compiler
                        COMMAND systest -n 6 --show-query-performance --groups Join --exclude-groups large CompilationIntensive --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${joinStrategy}_sliceCache_${enableSliceCache}_compiler_join --data ${EXPANDED_TEST_DATA_PATH}
                        --optimizer join_strategy=${joinStrategy}
                        --
                        --worker.query_engine.number_of_worker_threads=${workerThreads} --worker.default_query_execution.execution_mode=COMPILER --worker.number_of_buffers_in_global_buffer_manager=20000 --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
                set_tests_properties(systest_join_${workerThreads}_${joinStrategy}_sliceCache_${enableSliceCache}_compiler PROPERTIES LABELS "Systest" PROCESSORS ${workerThreads})
            endforeach ()
            ExternalData_Add_Test(test-data
                    NAME systest_agg_${workerThreads}_sliceCache_${enableSliceCache}_interpreter
                    COMMAND systest -n 6 --show-query-performance --groups Aggregation --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_sliceCache_${enableSliceCache}_interpreter_aggregation --data ${EXPANDED_TEST_DATA_PATH}
                    --
                    --worker.query_engine.number_of_worker_threads=${workerThreads} --worker.default_query_execution.execution_mode=INTERPRETER --worker.number_of_buffers_in_global_buffer_manager=20000 --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
            set_tests_properties(systest_agg_${workerThreads}_sliceCache_${enableSliceCache}_interpreter PROPERTIES LABELS "Systest" PROCESSORS ${workerThreads})
            ExternalData_Add_Test(test-data
                    NAME systest_agg_${workerThreads}_sliceCache_${enableSliceCache}_compiler
                    COMMAND systest -n 6 --show-query-performance --groups Aggregation --exclude-groups large CompilationIntensive --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_sliceCache_${enableSliceCache}_compiler_aggregation --data ${EXPANDED_TEST_DATA_PATH}
                    --
                    --worker.query_engine.number_of_worker_threads=${workerThreads} --worker.default_query_execution.execution_mode=COMPILER --worker.number_of_buffers_in_global_buffer_manager=20000 --worker.default_query_execution.slice_cache.enable_slice_cache=${enableSliceCache})
            set_tests_properties(systest_agg_${workerThreads}_sliceCache_${enableSliceCache}_compiler PROPERTIES LABELS "Systest" PROCESSORS ${workerThreads})
        endforeach ()
    endforeach ()
endif (NOT CODE_COVERAGE)

# Adding dependency for the nes-systest-lib so that the data is downloaded before the tests are run
ExternalData_Add_Target(test-data)
add_dependencies(nes-systest-lib test-data)

add_tests_if_enabled(tests)
