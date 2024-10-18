option(NES_SYSTEM_TEST_VERBOSE "Print additional debug information for system tests" OFF)

# Main entry point for setting up system-level tests
function(project_enable_system_level_tests)
    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    if (NOT Python3_FOUND)
        message(NOTICE "Disable System Tests. Could not find Python3.")
        return()
    endif()

    discover_tests()
    discover_test_groups()

    if (NOT TESTS_DISCOVERED OR NOT TEST_GROUPS_DISCOVERED)
        message(NOTICE "Unable to create system test targets. Could not find necessary test discovery files.")
        return()
    endif()

   file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/cache)

    create_test_targets()
    create_test_group_targets()
    copy_test_files_to_build_directory()

    message(STATUS "Enabled System Tests")
endfunction()


# We discover all tests and write them into tests/tests_discovery.json
function(discover_tests)
    execute_process(
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/llvm-lit/parse_tests.py
            --test_dirs ${CMAKE_SOURCE_DIR}/tests
            --file_endings ".test"
            --output ${CMAKE_BINARY_DIR}/nes-systests/tests_discovery.json
    )

    if (EXISTS ${CMAKE_BINARY_DIR}/nes-systests/tests_discovery.json)
        set(TESTS_DISCOVERED TRUE PARENT_SCOPE)
    else()
        set(TESTS_DISCOVERED FALSE PARENT_SCOPE)
    endif()
endfunction()

# We discover all test groups and write them into tests/test_groups_discovery.json
function(discover_test_groups)
    execute_process(
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/llvm-lit/parse_test_groups.py
            --test_dirs ${CMAKE_SOURCE_DIR}/tests
            --file_endings ".test"
            --output ${CMAKE_BINARY_DIR}/nes-systests/test_groups_discovery.json
    )

    if (EXISTS ${CMAKE_BINARY_DIR}/nes-systests/test_groups_discovery.json)
        set(TEST_GROUPS_DISCOVERED TRUE PARENT_SCOPE)
    else()
        set(TEST_GROUPS_DISCOVERED FALSE PARENT_SCOPE)
    endif()
endfunction()

# Create individual test targets
function(create_test_targets)
    file(READ ${CMAKE_BINARY_DIR}/nes-systests/tests_discovery.json TESTS_JSON)
    string(JSON TESTS_COUNT LENGTH ${TESTS_JSON})
    math(EXPR TESTS_COUNT_END "${TESTS_COUNT} - 1")

    foreach(INDEX RANGE 0 ${TESTS_COUNT_END})
        string(JSON TEST_FILEPATH GET ${TESTS_JSON} ${INDEX})
        get_filename_component(TEST_NAME ${TEST_FILEPATH} NAME_WE)
        message(STATUS "Found test ${TEST_NAME}")

        # Create the test target for local execution
        add_system_test(${TEST_FILEPATH} ${TEST_NAME})
    endforeach()
endfunction()

# Create test group targets
function(create_test_group_targets)
    file(READ "${CMAKE_BINARY_DIR}/nes-systests/test_groups_discovery.json" TEST_GROUPS_JSON)
    string(JSON GROUP_COUNT LENGTH ${TEST_GROUPS_JSON})
    math(EXPR GROUP_COUNT_END "${GROUP_COUNT} - 1")

    foreach(INDEX RANGE 0 ${GROUP_COUNT_END})
        string(JSON GROUP_NAME MEMBER ${TEST_GROUPS_JSON} ${INDEX})
        string(JSON GROUP_TESTS GET ${TEST_GROUPS_JSON} ${GROUP_NAME})
        message(STATUS "Found test group ${GROUP_NAME}")

        set(COMBINED_TEST_NAMES_SPACE "") # all test names separated by space --> used for local execution
        set(COMBINED_TEST_FILENAMES_SPACE "") # all test file names separated by space --> used for local execution
        set(COMBINED_TEST_FILENAMES_PIPE "") # all test file names separated by pipe --> used for filtering in lit

        string(JSON GROUP_TESTS_COUNT LENGTH ${GROUP_TESTS})
        math(EXPR GROUP_TESTS_COUNT_END "${GROUP_TESTS_COUNT} - 1")

        foreach(INDEX RANGE 0 ${GROUP_TESTS_COUNT_END})
            string(JSON GROUP_TEST_FILEPATH GET ${GROUP_TESTS} ${INDEX})
            get_filename_component(GROUP_TEST_NAME ${GROUP_TEST_FILEPATH} NAME_WE)

            if (INDEX EQUAL 0)
                set(COMBINED_TEST_NAMES_SPACE "${GROUP_TEST_NAME}")
                set(COMBINED_TEST_FILENAMES_SPACE "${GROUP_TEST_FILEPATH}")
                set(COMBINED_TEST_FILENAMES_PIPE "${GROUP_TEST_FILEPATH}")
            else()
                set(COMBINED_TEST_NAMES_SPACE "${COMBINED_TEST_NAMES_SPACE} ${GROUP_TEST_NAME}")
                set(COMBINED_TEST_FILENAMES_SPACE "${COMBINED_TEST_FILENAMES_SPACE} ${GROUP_TEST_FILEPATH}")
                set(COMBINED_TEST_FILENAMES_PIPE "${COMBINED_TEST_FILENAMES_PIPE}|${GROUP_TEST_FILEPATH}")
            endif()
        endforeach()

        # Create the test target for local execution
        add_system_test_group(${GROUP_NAME} ${COMBINED_TEST_FILENAMES_SPACE} ${COMBINED_TEST_NAMES_SPACE})
    endforeach()
endfunction()

# Copy necessary test data and scripts to the build directory
function(copy_test_files_to_build_directory)
    file(COPY ${CMAKE_SOURCE_DIR}/nes-systests/testdata DESTINATION ${CMAKE_BINARY_DIR}/tests)
endfunction()
