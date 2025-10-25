# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(GoogleTest)

# Target to build all integration tests
add_custom_target(integration_tests)
# Target to build all e2e tests
add_custom_target(e2e_tests)

# This function registers a test with gtest_discover_tests
function(add_nes_test)
    add_executable(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-test-util)
    if (NES_ENABLE_PRECOMPILED_HEADERS)
        target_precompile_headers(${TARGET_NAME} REUSE_FROM nes-common)
        # We need to compile with -fPIC to include with nes-common compiled headers as it uses PIC
        target_compile_options(${TARGET_NAME} PUBLIC "-fPIC")
    endif ()
    if (CODE_COVERAGE)
        target_code_coverage(${TARGET_NAME} PUBLIC AUTO ALL EXTERNAL OBJECTS nes nes-common)
        message(STATUS "Added cc test ${TARGET_NAME}")
    endif ()
    gtest_discover_tests(${TARGET_NAME} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} DISCOVERY_MODE PRE_TEST DISCOVERY_TIMEOUT 30)
    message(STATUS "Added ut ${TARGET_NAME}")
endfunction()

function(add_nes_common_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-common)
endfunction()

function(add_nes_unit_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-data-types)
endfunction()

function(add_nes_integration_test)
    # create a test executable that may contain multiple source files.
    # first param is TARGET_NAME
    add_executable(${ARGN})
    set(TARGET_NAME ${ARGV0})
    add_dependencies(integration_tests ${TARGET_NAME})
    if (NES_ENABLE_PRECOMPILED_HEADERS)
        target_precompile_headers(${TARGET_NAME} REUSE_FROM nes-common)
    endif ()
    target_link_libraries(${TARGET_NAME} nes-coordinator-test-util)
    gtest_discover_tests(${TARGET_NAME} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR}  DISCOVERY_MODE PRE_TEST DISCOVERY_TIMEOUT 30)
    message(STATUS "Added it test ${TARGET_NAME}")
endfunction()
