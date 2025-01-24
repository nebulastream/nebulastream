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

include(ExternalData)
set(ExternalData_URL_TEMPLATES
    "https://github.com/nebulastream/nes-datasets/releases/download/v1/%(algo)_%(hash)"
)

get_source(nes-systest-lib SLT_SOURCE_FILES)

add_library(nes-systest-lib STATIC ${SLT_SOURCE_FILES})
target_link_libraries(nes-systest-lib PUBLIC nes-nebuli-lib nes-single-node-worker-lib)
target_include_directories(nes-systest-lib PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_compile_definitions(nes-systest-lib PRIVATE
        PATH_TO_BINARY_DIR="${CMAKE_BINARY_DIR}"
        TEST_DISCOVER_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../"
        TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../testdata"
)

find_package(argparse CONFIG REQUIRED)

add_executable(systest src/SystestStarter.cpp)
target_compile_definitions(systest PRIVATE
        PATH_TO_BINARY_DIR="${CMAKE_BINARY_DIR}"
        TEST_DISCOVER_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../"
        TEST_DATA_DIR="${CMAKE_CURRENT_BINARY_DIR}/../testdata"
)
set_property(TARGET systest PROPERTY ENABLE_EXPORTS ON)
target_link_libraries(systest PUBLIC nes-systest-lib argparse::argparse)
target_include_directories(systest PUBLIC $<INSTALL_INTERFACE:/include/nebulastream>)
if (CODE_COVERAGE)
    target_code_coverage(systest COVERAGE_TARGET_NAME systest_interpreter PUBLIC AUTO ALL ARGS -n 20 --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_cc -- --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.queryEngine.numberOfWorkerThreads=8)
endif (CODE_COVERAGE)


include(${CMAKE_SOURCE_DIR}/cmake/CodeCoverage.cmake)

# If we are running code coverage, we need to ONLY run the interpreter tests, as otherwise, the code coverage will be 100% for all operators as the compiler traces all branches and operations.
ExternalData_Add_Test(TEST_DATA
        NAME systest_interpreter
        COMMAND systest -n 20 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter --exclude-groups large --data DATA{../testdata/,REGEX:.*} -- --worker.queryCompiler.nautilusBackend=INTERPRETER)
if (NOT CODE_COVERAGE)
    ExternalData_Add_Test(TEST_DATA
            NAME systest_compiler
            COMMAND systest -n 20 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/compiler --exclude-groups large --data DATA{../testdata/,REGEX:.*} -- --worker.queryCompiler.nautilusBackend=COMPILER)
endif (NOT CODE_COVERAGE)


if (ENABLE_LARGE_TESTS)
    ExternalData_Add_Test(TEST_DATA
            NAME systest_large
            COMMAND systest --sequential --workingDir=${CMAKE_CURRENT_BINARY_DIR}/large_scale --data DATA{../testdata/,RECURSE:,REGEX:.*} -- --worker.queryCompiler.nautilusBackend=COMPILER --worker.queryEngine.numberOfWorkerThreads=10)
endif ()
ExternalData_Add_Target(TEST_DATA)

# We run all tests with a small and large task queue size
# We set the number of worker threads to 8, to further stress the task queue
set(taskQueueSize 5 10000)
foreach (taskQueueSize IN LISTS taskQueueSize)
    if (CODE_COVERAGE)
        target_code_coverage(systest COVERAGE_TARGET_NAME systest_task_queue_size_${taskQueueSize}_interpreter PUBLIC AUTO ALL ARGS --sequential --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${taskQueueSize}_interpreter_cc -- --worker.queryEngine.numberOfWorkerThreads=8 --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=100000 --worker.taskQueueSize=${taskQueueSize})
        target_code_coverage(systest COVERAGE_TARGET_NAME systest_task_queue_size_${taskQueueSize}_compiler PUBLIC AUTO ALL ARGS --sequential --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${taskQueueSize}_compiler_cc -- --worker.queryEngine.numberOfWorkerThreads=8 --worker.queryCompiler.nautilusBackend=COMPILER --worker.numberOfBuffersInGlobalBufferManager=100000 --worker.taskQueueSize=${taskQueueSize})
    endif (CODE_COVERAGE)

    add_test(
            NAME systest_task_queue_size_${taskQueueSize}_interpreter
            COMMAND systest -n 100 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${taskQueueSize}_interpreter -- --worker.queryEngine.numberOfWorkerThreads=8 --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=100000 --worker.taskQueueSize=${taskQueueSize})
    add_test(
            NAME systest_task_queue_size_${taskQueueSize}_compiler
            COMMAND systest -n 100 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${taskQueueSize}_compiler -- --worker.queryEngine.numberOfWorkerThreads=8 --worker.queryCompiler.nautilusBackend=COMPILER --worker.numberOfBuffersInGlobalBufferManager=100000 --worker.taskQueueSize=${taskQueueSize})
endforeach ()

## We run all join and aggregation tests with different no. worker threads
set(workerThreads 1 2 4 8)
foreach (workerThreads IN LISTS workerThreads)
    if (CODE_COVERAGE)
        target_code_coverage(systest COVERAGE_TARGET_NAME systest_join_${workerThreads}_interpreter PUBLIC AUTO ALL ARGS -n 6 --groups Join --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_interpreter_join_cc -- --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=INTERPRETER)
        target_code_coverage(systest COVERAGE_TARGET_NAME systest_agg_${workerThreads}_interpreter PUBLIC AUTO ALL ARGS -n 6 --groups Aggregation --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_interpreter_aggregation_cc -- --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=INTERPRETER)
    endif (CODE_COVERAGE)

    add_test(
            NAME systest_join_${workerThreads}_interpreter
            COMMAND systest -n 6 --groups Join --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_interpreter_join
                    --
                    --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=INTERPRETER)
    add_test(
            NAME systest_join_${workerThreads}_compiler
            COMMAND systest -n 6 --groups Join --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_compiler_join
            --
            --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=COMPILER --worker.numberOfBuffersInGlobalBufferManager=20000)

    add_test(
            NAME systest_agg_${workerThreads}_interpreter
            COMMAND systest -n 6 --groups Aggregation --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_interpreter_aggregation
            --
            --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=20000)
    add_test(
            NAME systest_agg_${workerThreads}_compiler
            COMMAND systest -n 6 --groups Aggregation --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_compiler_aggregation
                    --
                    --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=COMPILER)
endforeach ()


function(add_nes_test_systest)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-systest-lib)
    target_include_directories(${TARGET_NAME}
            PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
endfunction()

add_tests_if_enabled(tests)
