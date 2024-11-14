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

# If we are running code coverage, we need to ONLY run the interpreter tests, as otherwise, the code coverage will be 100% for all operators as the compiler traces all branches and operations.
add_test(systest_interpreter systest --sequential --resultDir=${CMAKE_CURRENT_BINARY_DIR}/results_interpreter -- --worker.queryCompiler.nautilusBackend=INTERPRETER)
if (NOT CODE_COVERAGE)
    add_test(systest_compiler systest --sequential --resultDir=${CMAKE_CURRENT_BINARY_DIR}/results_compiler -- --worker.queryCompiler.nautilusBackend=COMPILER)
endif (NOT CODE_COVERAGE)

# We wish to run all join tests with a small, medium and large page size while also setting the threads to 1, 2, 4 for the worker
set(workerThreads 1 2 4 8)
foreach(workerThreads IN LISTS workerThreads)
    add_test(
            NAME systest_join_${workerThreads}_interpreter
            COMMAND systest --sequential --group Join --resultDir=${CMAKE_CURRENT_BINARY_DIR}/results_${workerThreads}_interpreter
                            --
                            --workerConfiguration.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=INTERPRETER)

    if (NOT CODE_COVERAGE)
        add_test(
                NAME systest_join_${workerThreads}_compiler
                COMMAND systest --sequential --group Join --resultDir=${CMAKE_CURRENT_BINARY_DIR}/results_${workerThreads}_compiler
                                --
                                --workerConfiguration.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=COMPILER)
    endif (NOT CODE_COVERAGE)
endforeach()

function(add_nes_test_systest)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-systest-lib)
    target_include_directories(${TARGET_NAME}
            PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
endfunction()

add_tests_if_enabled(tests)
