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
    target_code_coverage(systest COVERAGE_TARGET_NAME systest_interpreter PUBLIC AUTO ALL ARGS --sequential --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_cc -- --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=20000 --worker.queryEngine.numberOfWorkerThreads=8)
endif (CODE_COVERAGE)


include(${CMAKE_SOURCE_DIR}/cmake/CodeCoverage.cmake)

# If we are running code coverage, we need to ONLY run the interpreter tests, as otherwise, the code coverage will be 100% for all operators as the compiler traces all branches and operations.
add_test(NAME systest_interpreter COMMAND systest -n 6 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter -- --worker.queryEngine.numberOfWorkerThreads=8 --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=20000)
add_test(NAME systest_compiler COMMAND systest -n 6 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/compiler -- --worker.queryEngine.numberOfWorkerThreads=8 --worker.queryCompiler.nautilusBackend=COMPILER --worker.numberOfBuffersInGlobalBufferManager=20000)

## We wish to run all join and aggregation tests with different no. worker threads
## Additionally, we wish to run all join and aggregation tests with all available slice caches, three different slice caches and etc.
set(workerThreads 1 4 8)
set(gatherSlices false true)
set(sortBufferByFields false true)
set(sliceCaches NONE LRU FIFO SECOND_CHANCE)
set(numberOfEntriesSliceCaches 1 10 100 1000)
set(shuffleStrategies NONE BUFFER TUPLES BUFFER_TUPLES)
foreach (gatherSlice IN LISTS gatherSlices)
    foreach (sortBufferByField IN LISTS sortBufferByFields)
        foreach (workerThreads IN LISTS workerThreads)
            foreach (sliceCache IN LISTS sliceCaches)
                foreach (cacheSize IN LISTS numberOfEntriesSliceCaches)
                    foreach (shuffleStrategy IN LISTS shuffleStrategies)
                        if (CODE_COVERAGE)
                            target_code_coverage(systest COVERAGE_TARGET_NAME systest_join_${workerThreads}_interpreter PUBLIC AUTO ALL ARGS --sequential --groups Join --workingDir=${CMAKE_CURRENT_BINARY_DIR}/results_${workerThreads}_interpreter_join -- --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=100000)
                            target_code_coverage(systest COVERAGE_TARGET_NAME systest_agg_${workerThreads}_interpreter PUBLIC AUTO ALL ARGS --sequential --groups Aggregation --workingDir=${CMAKE_CURRENT_BINARY_DIR}/results_${workerThreads}_interpreter_aggregation -- --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=100000)
                        endif (CODE_COVERAGE)

                        add_test(
                                NAME systest_join_${workerThreads}_${sliceCache}_${cacheSize}_${shuffleStrategy}_${gatherSlice}_${sortBufferByField}_interpreter
                                COMMAND systest -n 6 --groups Join --workingDir=${CMAKE_CURRENT_BINARY_DIR}/results_${workerThreads}_${sliceCache}_${cacheSize}_${shuffleStrategy}_${gatherSlice}_${sortBufferByField}_interpreter_join
                                --
                                --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=20000 --worker.queryCompiler.sliceCacheType=${sliceCache} --worker.queryCompiler.sortBufferByField=${sortBufferByField} --worker.queryCompiler.gatherSlices=${gatherSlices} --worker.queryCompiler.numberOfEntriesSliceCache=${cacheSize} --worker.queryCompiler.shuffleStrategy=${shuffleStrategy} --worker.queryCompiler.degreeOfDisorder=1.0)
                        add_test(
                                NAME systest_join_${workerThreads}_${sliceCache}_${cacheSize}_${shuffleStrategy}_${gatherSlice}_${sortBufferByField}_compiler
                                COMMAND systest -n 6 --groups Join --workingDir=${CMAKE_CURRENT_BINARY_DIR}/results_${workerThreads}_${sliceCache}_${cacheSize}_${shuffleStrategy}_${gatherSlice}_${sortBufferByField}_compiler_join
                                --
                                --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=COMPILER --worker.numberOfBuffersInGlobalBufferManager=20000 --worker.queryCompiler.sliceCacheType=${sliceCache} --worker.queryCompiler.sortBufferByField=${sortBufferByField} --worker.queryCompiler.gatherSlices=${gatherSlices} --worker.queryCompiler.numberOfEntriesSliceCache=${cacheSize} --worker.queryCompiler.shuffleStrategy=${shuffleStrategy} --worker.queryCompiler.degreeOfDisorder=1.0)

                        add_test(
                                NAME systest_agg_${workerThreads}_${sliceCache}_${cacheSize}_${shuffleStrategy}_${gatherSlice}_${sortBufferByField}_interpreter
                                COMMAND systest -n 6 --groups Aggregation --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${sliceCache}_${cacheSize}_${shuffleStrategy}_${gatherSlice}_${sortBufferByField}_interpreter_aggregation
                                --
                                --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=20000 --worker.queryCompiler.sliceCacheType=${sliceCache} --worker.queryCompiler.sortBufferByField=${sortBufferByField} --worker.queryCompiler.gatherSlices=${gatherSlices} --worker.queryCompiler.numberOfEntriesSliceCache=${cacheSize} --worker.queryCompiler.shuffleStrategy=${shuffleStrategy} --worker.queryCompiler.degreeOfDisorder=1.0)
                        add_test(
                                NAME systest_agg_${workerThreads}_${sliceCache}_${cacheSize}_${shuffleStrategy}_${gatherSlice}_${sortBufferByField}_compiler
                                COMMAND systest -n 6 --groups Aggregation --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${sliceCache}_${cacheSize}_${shuffleStrategy}_${gatherSlice}_${sortBufferByField}_compiler_aggregation
                                --
                                --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryCompiler.nautilusBackend=COMPILER --worker.numberOfBuffersInGlobalBufferManager=20000 --worker.queryCompiler.sliceCacheType=${sliceCache} --worker.queryCompiler.sortBufferByField=${sortBufferByField} --worker.queryCompiler.gatherSlices=${gatherSlices} --worker.queryCompiler.numberOfEntriesSliceCache=${cacheSize} --worker.queryCompiler.shuffleStrategy=${shuffleStrategy} --worker.queryCompiler.degreeOfDisorder=1.0)

                    endforeach ()
                endforeach ()
            endforeach ()
        endforeach ()
    endforeach ()
endforeach ()

function(add_nes_test_systest)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-systest-lib)
    target_include_directories(${TARGET_NAME}
            PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
endfunction()

add_tests_if_enabled(tests)
