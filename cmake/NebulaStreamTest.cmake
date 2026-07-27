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
find_package(rapidcheck CONFIG REQUIRED)

# RapidCheck iteration count applied to all tests via RC_PARAMS.
# Set NES_EXHAUSTIVE_TESTS=ON for thorough testing (100 iterations), default is fast (20 iterations).
option(NES_EXHAUSTIVE_TESTS "Run more RapidCheck iterations for thorough property testing" OFF)
if (NES_EXHAUSTIVE_TESTS)
    set(RAPIDCHECK_MAX_SUCCESS 100)
else ()
    set(RAPIDCHECK_MAX_SUCCESS 20)
endif ()

option(NES_SPLIT_TEST_BINARIES "Build each unit-test suite as a separate executable" ON)

# Target to build all integration tests
add_custom_target(integration_tests)
# Target to build all e2e tests
add_custom_target(e2e_tests)

function(configure_nes_test_target TARGET_NAME)
    target_link_libraries(${TARGET_NAME} nes-test-util rapidcheck rapidcheck_gtest)
    if (NES_ENABLE_PRECOMPILED_HEADERS)
        target_precompile_headers(${TARGET_NAME} REUSE_FROM nes-common)
        # We need to compile with -fPIC to include with nes-common compiled headers as it uses PIC
        target_compile_options(${TARGET_NAME} PUBLIC "-fPIC")
    endif ()
    if (CODE_COVERAGE)
        target_code_coverage(${TARGET_NAME} PUBLIC AUTO ALL EXTERNAL OBJECTS nes nes-common)
        message(STATUS "Added cc test ${TARGET_NAME}")
    endif ()
endfunction()

function(discover_nes_test TARGET_NAME TEST_WORKING_DIRECTORY)
    gtest_discover_tests(${TARGET_NAME}
        WORKING_DIRECTORY ${TEST_WORKING_DIRECTORY}
        DISCOVERY_MODE PRE_TEST
        DISCOVERY_TIMEOUT 30
        PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=${RAPIDCHECK_MAX_SUCCESS}")
    message(STATUS "Added ut ${TARGET_NAME}")
endfunction()

function(finalize_nes_combined_test TARGET_NAME)
    get_target_property(TEST_OBJECT_TARGETS ${TARGET_NAME} NES_TEST_OBJECT_TARGETS)
    foreach (TEST_OBJECT_TARGET ${TEST_OBJECT_TARGETS})
        target_link_libraries(${TARGET_NAME} PRIVATE ${TEST_OBJECT_TARGET})
    endforeach ()
    if (CODE_COVERAGE)
        target_code_coverage(${TARGET_NAME} PUBLIC AUTO ALL EXTERNAL OBJECTS nes nes-common)
    endif ()
    discover_nes_test(${TARGET_NAME} ${CMAKE_BINARY_DIR})
endfunction()

# This function registers a test with gtest_discover_tests. The combined layout compiles each suite as an object library
# so that suite-specific compile settings remain isolated while the static NES libraries are linked only once.
function(add_nes_test TARGET_NAME)
    if (NES_SPLIT_TEST_BINARIES)
        add_executable(${TARGET_NAME} ${ARGN})
        configure_nes_test_target(${TARGET_NAME})
        discover_nes_test(${TARGET_NAME} ${CMAKE_CURRENT_BINARY_DIR})
        return()
    endif ()

    add_library(${TARGET_NAME} OBJECT ${ARGN})
    configure_nes_test_target(${TARGET_NAME})

    if (NOT TARGET nes-unit-tests)
        add_executable(nes-unit-tests)
        cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}" CALL finalize_nes_combined_test nes-unit-tests)
    endif ()
    set_property(TARGET nes-unit-tests APPEND PROPERTY NES_TEST_OBJECT_TARGETS ${TARGET_NAME})
    message(STATUS "Added ut ${TARGET_NAME} to nes-unit-tests")
endfunction()

function(add_nes_common_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    find_package(reflectcpp CONFIG REQUIRED)
    target_link_libraries(${TARGET_NAME} nes-common reflectcpp::reflectcpp)
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
