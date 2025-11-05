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
target_link_libraries(nes-systest-lib PUBLIC nes-frontend-embedded-lib nes-single-node-worker-lib nes-sql-parser PRIVATE nes-input-formatters)
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
target_link_libraries(systest PUBLIC nes-systest-lib argparse::argparse)
target_include_directories(systest PUBLIC $<INSTALL_INTERFACE:/include/nebulastream>)

# Add code coverage if enabled
if (CODE_COVERAGE)
    target_code_coverage(systest COVERAGE_TARGET_NAME systest_interpreter_coverage PUBLIC AUTO ALL ARGS -n 20 --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_cc --data ${EXPANDED_TEST_DATA_PATH} -- --worker.default_query_execution.execution_mode=INTERPRETER --worker.default_query_execution.join_strategy=NESTED_LOOP_JOIN --enable_event_trace=true)
    target_code_coverage(systest COVERAGE_TARGET_NAME systest_interpreter_coverage_Hash_Join PUBLIC AUTO ALL ARGS -n 20 --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_cc --data ${EXPANDED_TEST_DATA_PATH} -- --worker.default_query_execution.execution_mode=INTERPRETER --worker.default_query_execution.join_strategy=HASH_JOIN --enable_event_trace=true)
    # Make sure to fetch ExternalData before running the ccov targets
    add_dependencies(ccov-run-systest_interpreter_coverage test-data)
    add_dependencies(ccov-run-systest_interpreter_coverage_Hash_Join test-data)
endif (CODE_COVERAGE)

# If we are running code coverage, we need to ONLY run the interpreter tests, as otherwise, the code coverage will be 100% for all operators as the compiler traces all branches and operations.
set(joinStrategies NESTED_LOOP_JOIN HASH_JOIN)
foreach (joinStrategy IN LISTS joinStrategies)
    ExternalData_Add_Test(test-data
            NAME systest_interpreter_${joinStrategy}
            COMMAND systest -n 20 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_${joinStrategy} --exclude-groups large --data ${EXPANDED_TEST_DATA_PATH} -- --worker.default_query_execution.execution_mode=INTERPRETER --worker.default_query_execution.join_strategy=${joinStrategy})
endforeach ()
if (NOT CODE_COVERAGE)
    ExternalData_Add_Test(test-data
            NAME systest_compiler
            COMMAND systest -n 20 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/compiler --exclude-groups large CompilationIntensive --data ${EXPANDED_TEST_DATA_PATH} -- --worker.default_query_execution.execution_mode=COMPILER --enable_event_trace=true)
endif (NOT CODE_COVERAGE)


if (ENABLE_LARGE_TESTS)
    message(STATUS "Large scale tests enabled")

    set(joinStrategy HASH_JOIN) # We simply test here with a hash join, as a NLJ does not finish in 2h
    ExternalData_Add_Test(test-data
            # currently, 4 worker threads perform the best on the large systests (avoids lock contention on NEXMARK Query 5)
            NAME systest_large_${joinStrategy}
            COMMAND systest -n 6 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/large_scale_${joinStrategy} --data ${EXPANDED_TEST_DATA_PATH} --groups large -- --worker.default_query_execution.execution_mode=COMPILER --worker.query_engine.number_of_worker_threads=4 --worker.number_of_buffers_in_global_buffer_manager=2000000 --worker.default_query_execution.join_strategy=${joinStrategy})

    ExternalData_Add_Test(test-data
            # currently, 4 worker threads perform the best on the large systests (avoids lock contention on NEXMARK Query 5)
            NAME systest_large_benchmark_${joinStrategy}
            COMMAND systest -b --workingDir=${CMAKE_CURRENT_BINARY_DIR}/large_scale_benchmark_${joinStrategy} --data ${EXPANDED_TEST_DATA_PATH} --groups large -- --worker.default_query_execution.execution_mode=COMPILER --worker.query_engine.number_of_worker_threads=16 --worker.number_of_buffers_in_global_buffer_manager=2000000 --worker.default_query_execution.join_strategy=${joinStrategy})


    # Running here the Nexmark.test queries with NLJ to test it for larger data sets but the input data size
    # is still small enough to finish the test in minutes.
    set(joinStrategy NESTED_LOOP_JOIN)
    ExternalData_Add_Test(TEST_DATA_LARGE
            NAME systest_large_${joinStrategy}
            COMMAND systest -n 6 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/large_scale_${joinStrategy} --data ${EXPANDED_TEST_DATA_PATH} -t ${CMAKE_CURRENT_SOURCE_DIR}/../benchmark/Nexmark.test:05 -- --worker.default_query_execution.execution_mode=COMPILER --worker.query_engine.number_of_worker_threads=4 --worker.number_of_buffers_in_global_buffer_manager=2000000 --worker.default_query_execution.join_strategy=${joinStrategy})
endif ()


if (NOT CODE_COVERAGE)
    ## We run all join and aggregation tests with different no. worker threads and different join strategies
    set(workerThreads 1 2 4)
    set(joinStrategies NESTED_LOOP_JOIN HASH_JOIN)
    foreach (workerThreads IN LISTS workerThreads)
        foreach (joinStrategy IN LISTS joinStrategies)
            ExternalData_Add_Test(test-data
                    NAME systest_join_${workerThreads}_${joinStrategy}_interpreter
                    COMMAND systest -n 6 --groups Join --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${joinStrategy}_interpreter_join --data ${EXPANDED_TEST_DATA_PATH}
                    --
                    --worker.query_engine.number_of_worker_threads=${workerThreads} --worker.default_query_execution.execution_mode=INTERPRETER --worker.number_of_buffers_in_global_buffer_manager=20000 --worker.default_query_execution.join_strategy=${joinStrategy})
            ExternalData_Add_Test(test-data
                    NAME systest_join_${workerThreads}_${joinStrategy}_compiler
                    COMMAND systest -n 6 --groups Join --exclude-groups large CompilationIntensive --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${joinStrategy}_compiler_join --data ${EXPANDED_TEST_DATA_PATH}
                    --
                    --worker.query_engine.number_of_worker_threads=${workerThreads} --worker.default_query_execution.execution_mode=COMPILER --worker.number_of_buffers_in_global_buffer_manager=20000 --worker.default_query_execution.join_strategy=${joinStrategy})
        endforeach ()
        ExternalData_Add_Test(test-data
                NAME systest_agg_${workerThreads}_interpreter
                COMMAND systest -n 6 --groups Aggregation --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_interpreter_aggregation --data ${EXPANDED_TEST_DATA_PATH}
                --
                --worker.query_engine.number_of_worker_threads=${workerThreads} --worker.default_query_execution.execution_mode=INTERPRETER --worker.number_of_buffers_in_global_buffer_manager=20000)
        ExternalData_Add_Test(test-data
                NAME systest_agg_${workerThreads}_compiler
                COMMAND systest -n 6 --groups Aggregation --exclude-groups large CompilationIntensive --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_compiler_aggregation --data ${EXPANDED_TEST_DATA_PATH}
                --
                --worker.query_engine.number_of_worker_threads=${workerThreads} --worker.default_query_execution.execution_mode=COMPILER --worker.number_of_buffers_in_global_buffer_manager=20000)
    endforeach ()
endif (NOT CODE_COVERAGE)

# Adding dependency for the nes-systest-lib so that the data is downloaded before the tests are run
ExternalData_Add_Target(test-data)
add_dependencies(nes-systest-lib test-data)

add_tests_if_enabled(tests)

if (ENABLE_DOCKER_TESTS)
    add_test(
            NAME systest-remote-test
            COMMAND
            env SYSTEST=$<TARGET_FILE:systest>
            env NES_DIR=${CMAKE_SOURCE_DIR}
            env NEBULASTREAM=$<TARGET_FILE:nes-single-node-worker>
            ${BATS} -x --verbose-run ${CMAKE_CURRENT_SOURCE_DIR}/remote-test/systest_remote_test.bats
    )
    set_tests_properties(systest-remote-test PROPERTIES LABELS "DockerCompose")
endif ()
