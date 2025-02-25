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
        TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../testdata"
)
set_property(TARGET systest PROPERTY ENABLE_EXPORTS ON)
target_link_libraries(systest PUBLIC nes-systest-lib argparse::argparse)
target_include_directories(systest PUBLIC $<INSTALL_INTERFACE:/include/nebulastream>)
if (CODE_COVERAGE)
    target_code_coverage(systest COVERAGE_TARGET_NAME systest_interpreter PUBLIC AUTO ALL ARGS -n 100 --resultDir=${CMAKE_CURRENT_BINARY_DIR}/results_interpreter -- --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=20000 --worker.queryEngine.numberOfWorkerThreads=8)
endif (CODE_COVERAGE)


include(${CMAKE_SOURCE_DIR}/cmake/CodeCoverage.cmake)

# If we are running code coverage, we need to ONLY run the interpreter tests, as otherwise, the code coverage will be 100% for all operators as the compiler traces all branches and operations.
add_test(NAME systest_interpreter COMMAND systest -n 100 --resultDir=${CMAKE_CURRENT_BINARY_DIR}/results_interpreter -- --worker.queryEngine.numberOfWorkerThreads=8 --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=20000)
add_test(NAME systest_compiler COMMAND systest -n 100 --resultDir=${CMAKE_CURRENT_BINARY_DIR}/results_compiler -- --worker.queryEngine.numberOfWorkerThreads=8 --worker.queryCompiler.nautilusBackend=COMPILER --worker.numberOfBuffersInGlobalBufferManager=20000)


## We wish to run all join tests with 1, 2, 4 worker threads
set(workerThreads 1 2 4 8)
foreach(workerThreads IN LISTS workerThreads)
    if (CODE_COVERAGE)
        target_code_coverage(systest COVERAGE_TARGET_NAME systest_join_${workerThreads}_interpreter PUBLIC AUTO ALL ARGS -n 6 --group Join --resultDir=${CMAKE_CURRENT_BINARY_DIR}/results_${workerThreads}_interpreter -- --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=20000)
    endif (CODE_COVERAGE)

    add_test(
            NAME systest_join_${workerThreads}_interpreter
            COMMAND systest -n 6 --group Join --resultDir=${CMAKE_CURRENT_BINARY_DIR}/results_${workerThreads}_interpreter
                            --
                            --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=20000)
    add_test(
            NAME systest_join_${workerThreads}_compiler
            COMMAND systest -n 6 --group Join --resultDir=${CMAKE_CURRENT_BINARY_DIR}/results_${workerThreads}_compiler
                            --
                            --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=COMPILER --worker.numberOfBuffersInGlobalBufferManager=20000)
endforeach()


function(add_nes_test_systest)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-systest-lib)
    target_include_directories(${TARGET_NAME}
            PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
endfunction()

add_tests_if_enabled(tests)
