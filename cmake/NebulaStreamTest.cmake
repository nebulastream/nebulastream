include(GoogleTest)
if (NES_CODE_COVERAGE)
    set(CODE_COVERAGE ON)
endif ()
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

function(enable_extra_test_features)
    if (CODE_COVERAGE)
        add_code_coverage_all_targets(EXCLUDE ${CMAKE_BINARY_DIR}/* tests/*)
    endif ()
endfunction()
