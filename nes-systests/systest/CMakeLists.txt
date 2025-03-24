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
include(ExternalData)
set(ExternalData_URL_TEMPLATES
        "https://github.com/nebulastream/nes-datasets/releases/download/v1/%(algo)_%(hash)"
)
set(ExternalData_TIMEOUT_ABSOLUTE 600)

get_source(nes-systest-lib SLT_SOURCE_FILES)

add_library(nes-systest-lib STATIC ${SLT_SOURCE_FILES})
target_link_libraries(nes-systest-lib PUBLIC nes-nebuli-lib nes-single-node-worker-lib)
target_include_directories(nes-systest-lib PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_compile_definitions(nes-systest-lib PRIVATE
        PATH_TO_BINARY_DIR="${CMAKE_BINARY_DIR}"
        TEST_DISCOVER_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../"
        TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../testdata"
)

# Find required packages
find_package(argparse CONFIG REQUIRED)

# Add the systest executable
add_executable(systest src/SystestStarter.cpp)
target_compile_definitions(systest PRIVATE
        PATH_TO_BINARY_DIR="${CMAKE_BINARY_DIR}"
        TEST_DISCOVER_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../"
        TEST_DATA_DIR="${CMAKE_CURRENT_BINARY_DIR}/../testdata"
)
set_property(TARGET systest PROPERTY ENABLE_EXPORTS ON)
target_link_libraries(systest PUBLIC nes-systest-lib argparse::argparse)
target_include_directories(systest PUBLIC $<INSTALL_INTERFACE:/include/nebulastream>)
set(memoryLayoutSelections "FIXED" "NONE")
set(SMALL_DATA DATA{../testdata/,RECURSE:,REGEX:small.*})
set(LARGE_DATA DATA{../testdata/,RECURSE:,REGEX:large.*})

foreach (memoryLayoutSelection IN LISTS memoryLayoutSelections)
    # If the memory layout type is fixed, we would like to run all tests with row and column layout. Otherwise, we run all tests with the default layout.
    if (memoryLayoutSelection STREQUAL "FIXED")
        set(memoryLayouts "ROW_LAYOUT" "COLUMNAR_LAYOUT")
    else ()
        # We have to set any memory layout that is supported, even though we do not use it.
        # We would not need to set the memory layout for the NONE MemoryLayoutSelectionPhase, but our configurations do not support
        # configurations depending on other configuration values.
        set(memoryLayouts "ROW_LAYOUT")
    endif ()

    foreach (memoryLayout IN LISTS memoryLayouts)
        message(STATUS "Running tests with memory layout selection: ${memoryLayoutSelection} and memory layout: ${memoryLayout}")

        # Add code coverage if enabled
        if (CODE_COVERAGE)
            # Can't use ExternalData_Add_Test here. This code manually expands the DATA{...} to the real path.
            ExternalData_Expand_Arguments(
                    TEST_DATA
                    TEST_DATA_PATH
                    ${SMALL_DATA}
            )
            target_code_coverage(systest COVERAGE_TARGET_NAME systest_interpreter_coverage_${memoryLayoutSelection}_${memoryLayout} PUBLIC AUTO ALL ARGS -n 20 --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_cc_${memoryLayoutSelection}_${memoryLayout} --data ${TEST_DATA_PATH} -- --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.queryCompiler.memoryLayoutType=${memoryType} --worker.queryEngine.taskQueueSize=100000)
            # Make sure to fetch ExternalData before running the ccov targets
            add_dependencies(ccov-run-systest_interpreter_coverage_${memoryLayoutSelection}_${memoryLayout}  TEST_DATA)
        endif (CODE_COVERAGE)

        # If we are running code coverage, we need to ONLY run the interpreter tests, as otherwise, the code coverage will be 100% for all operators as the compiler traces all branches and operations.
        ExternalData_Add_Test(TEST_DATA
                NAME systest_interpreter_${memoryLayoutSelection}_${memoryLayout}
                COMMAND systest -n 20 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${memoryLayoutSelection}_${memoryLayout}_interpreter --exclude-groups large --data ${SMALL_DATA} -- --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.queryEngine.taskQueueSize=100000 --worker.queryCompiler.memoryLayoutType=${memoryLayout} --worker.queryCompiler.memorySelectionPhase=${memoryLayoutSelection})
        if (NOT CODE_COVERAGE)
            ExternalData_Add_Test(TEST_DATA
                    NAME systest_compiler_${memoryLayoutSelection}_${memoryLayout}
                    COMMAND systest -n 20 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${memoryLayoutSelection}_${memoryLayout}_compiler --exclude-groups large --data ${SMALL_DATA} -- --worker.queryCompiler.nautilusBackend=COMPILER --worker.queryEngine.taskQueueSize=100000 --worker.queryCompiler.memoryLayoutType=${memoryLayout} --worker.queryCompiler.memorySelectionPhase=${memoryLayoutSelection})
        endif (NOT CODE_COVERAGE)


        if (ENABLE_LARGE_TESTS)
            ExternalData_Add_Test(TEST_DATA_LARGE
                    NAME systest_large_${memoryLayoutSelection}_${memoryLayout}
                    COMMAND systest -n 6 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${memoryLayoutSelection}_${memoryLayout}large_scale --data ${LARGE_DATA} --groups large -- --worker.queryCompiler.nautilusBackend=COMPILER --worker.queryEngine.numberOfWorkerThreads=10 --worker.queryEngine.taskQueueSize=100000 --worker.numberOfBuffersInGlobalBufferManager=200000 --worker.queryCompiler.memoryLayoutType=${memoryLayout}  --worker.queryCompiler.memorySelectionPhase=${memoryLayoutSelection})
            message(STATUS "Large scale tests enabled")
        endif ()


        if (NOT CODE_COVERAGE)
            # We run all tests with some small and one large task queue size
            # We set the number of worker threads to 8, to further stress the task queue
            set(taskQueueSize 100 10000)
            foreach (taskQueueSize IN LISTS taskQueueSize)
                ExternalData_Add_Test(TEST_DATA
                        NAME systest_task_queue_size_${taskQueueSize}_${memoryLayoutSelection}_${memoryLayout}_interpreter
                        COMMAND systest -n 4 --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${taskQueueSize}_${memoryLayoutSelection}_${memoryLayout}_interpreter --data ${SMALL_DATA} -- --worker.queryEngine.numberOfWorkerThreads=4 --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=200000 --worker.queryEngine.taskQueueSize=${taskQueueSize} --worker.queryCompiler.memoryLayoutType=${memoryLayout}  --worker.queryCompiler.memorySelectionPhase=${memoryLayoutSelection})
                ExternalData_Add_Test(TEST_DATA
                        NAME systest_task_queue_size_${taskQueueSize}_${memoryLayoutSelection}_${memoryLayout}_compiler
                        COMMAND systest -n 4 --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${taskQueueSize}_${memoryLayoutSelection}_${memoryLayout}_compiler --data ${SMALL_DATA} -- --worker.queryEngine.numberOfWorkerThreads=4 --worker.queryCompiler.nautilusBackend=COMPILER --worker.numberOfBuffersInGlobalBufferManager=200000 --worker.queryEngine.taskQueueSize=${taskQueueSize} --worker.queryCompiler.memoryLayoutType=${memoryLayout}  --worker.queryCompiler.memorySelectionPhase=${memoryLayoutSelection})
            endforeach ()

            ## We run all join and aggregation tests with different no. worker threads
            set(workerThreads 1 2 4 8)
            foreach (workerThreads IN LISTS workerThreads)
                ExternalData_Add_Test(TEST_DATA
                        NAME systest_join_${workerThreads}_${memoryLayoutSelection}_${memoryLayout}_interpreter
                        COMMAND systest -n 6 --groups Join --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${memoryLayoutSelection}_${memoryLayout}_interpreter_join --data ${SMALL_DATA}
                        --
                        --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=20000 --worker.queryCompiler.memoryLayoutType=${memoryLayout}  --worker.queryCompiler.memorySelectionPhase=${memoryLayoutSelection})
                ExternalData_Add_Test(TEST_DATA
                        NAME systest_join_${workerThreads}_${memoryLayoutSelection}_${memoryLayout}_compiler
                        COMMAND systest -n 6 --groups Join --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${memoryLayoutSelection}_${memoryLayout}_compiler_join --data ${SMALL_DATA}
                        --
                        --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=COMPILER --worker.numberOfBuffersInGlobalBufferManager=20000 --worker.queryCompiler.memoryLayoutType=${memoryLayout}  --worker.queryCompiler.memorySelectionPhase=${memoryLayoutSelection})

                ExternalData_Add_Test(TEST_DATA
                        NAME systest_agg_${workerThreads}_${memoryLayoutSelection}_${memoryLayout}_interpreter
                        COMMAND systest -n 6 --groups Aggregation --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${memoryLayoutSelection}_${memoryLayout}_interpreter_aggregation --data ${SMALL_DATA}
                        --
                        --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=20000 --worker.queryCompiler.memoryLayoutType=${memoryLayout}  --worker.queryCompiler.memorySelectionPhase=${memoryLayoutSelection})
                ExternalData_Add_Test(TEST_DATA
                        NAME systest_agg_${workerThreads}_${memoryLayoutSelection}_${memoryLayout}_compiler
                        COMMAND systest -n 6 --groups Aggregation --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${memoryLayoutSelection}_${memoryLayout}_compiler_aggregation --data ${SMALL_DATA}
                        --
                        --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=COMPILER --worker.numberOfBuffersInGlobalBufferManager=20000 --worker.queryCompiler.memoryLayoutType=${memoryLayout}  --worker.queryCompiler.memorySelectionPhase=${memoryLayoutSelection})
            endforeach ()
        endif (NOT CODE_COVERAGE)
    endforeach()
endforeach()

# Adding dependency for the nes-systest-lib so that the data is downloaded before the tests are run
add_dependencies(nes-systest-lib TEST_DATA)
ExternalData_Add_Target(TEST_DATA SHOW_PROGRESS ON)
if (ENABLE_LARGE_TESTS)
    add_dependencies(nes-systest-lib TEST_DATA_LARGE)
    ExternalData_Add_Target(TEST_DATA_LARGE SHOW_PROGRESS ON)
endif ()


function(add_nes_test_systest)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-systest-lib)
    target_include_directories(${TARGET_NAME}
            PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
endfunction()

add_tests_if_enabled(tests)
