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
target_compile_definitions(nes-systest-lib PUBLIC PATH_TO_BINARY_DIR="${CMAKE_BINARY_DIR}")
target_compile_definitions(nes-systest-lib PUBLIC CACHE_DIR="${CMAKE_BINARY_DIR}/cache/")
target_compile_definitions(nes-systest-lib PUBLIC TEST_DISCOVER_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../")

find_package(argparse CONFIG REQUIRED)

add_executable(systest src/SystestStarter.cpp)
set_property(TARGET systest PROPERTY ENABLE_EXPORTS ON)
target_link_libraries(systest PUBLIC nes-systest-lib argparse::argparse)
target_include_directories(systest PUBLIC $<INSTALL_INTERFACE:/include/nebulastream>)

# If we are running code coverage, we need to ONLY run the interpreter tests, as otherwise, the code coverage will be 100% for all operators
# as the compiler traces all branches and operations.
# As there is some problem if multiple queries get executed simultaneously, we have to run the tests sequentially.
if (CODE_COVERAGE)
    add_test(systest_interpreter systest --sequential -- --queryCompilerConfiguration.queryCompilerNautilusBackendConfig=INTERPRETER)
    target_code_coverage(systest_interpreter PUBLIC AUTO ALL EXTERNAL OBJECTS nes nes-compiler nes-common)
else ()
    add_test(systest_interpreter systest --sequential -- --queryCompilerConfiguration.queryCompilerNautilusBackendConfig=INTERPRETER)
    add_test(systest_compiler systest --sequential -- --queryCompilerConfiguration.queryCompilerNautilusBackendConfig=COMPILER)
endif (CODE_COVERAGE)



function(add_nes_test_systest)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-systest-lib)
    target_include_directories(${TARGET_NAME}
            PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
endfunction()

add_tests_if_enabled(tests)
