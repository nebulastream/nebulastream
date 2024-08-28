# This file provides functionality to enable system level tests using llvm-lit

option(NES_SYSTEM_TEST_VERBOSE "Print additional debug information for system tests" ON)

# Setup system level tests by introducing llvm lit and cmake targets
function(project_enable_system_level_tests)

    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    if (EXISTS ${Python3_EXECUTABLE})
        if (NES_ENABLE_LLVM_LIT)
            # Loading dependencies. We might want to ship them with vcpkg in the future
            set(VENV_DIR "${CMAKE_BINARY_DIR}/venv")
            if(EXISTS "${VENV_DIR}/bin/lit")
                message(STATUS "Use llvm-lit in python environment at " ${VENV_DIR})
            else()
                execute_process(COMMAND ${Python3_EXECUTABLE} -m venv ${VENV_DIR})
                execute_process(COMMAND ${VENV_DIR}/bin/pip install --upgrade pip)
                execute_process(COMMAND ${VENV_DIR}/bin/pip install lit)
                message(STATUS "Created python virtual environment with llvm-lit at " ${VENV_DIR})
            endif()
        endif()

        # TODO: Investigate if we can use lit here -- show all tests by feature
        # Discover tests and test groups
        execute_process(COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/llvm-lit/parse_tests.py --test_dirs ${CMAKE_SOURCE_DIR}/test --file_endings ".test" --output ${CMAKE_BINARY_DIR}/tests.json)
        execute_process(COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/llvm-lit/parse_test_groups.py --test_dirs ${CMAKE_SOURCE_DIR}/test --file_endings ".test" --output ${CMAKE_BINARY_DIR}/test_groups.json)

        if(EXISTS "${CMAKE_BINARY_DIR}/tests.json" AND EXISTS "${CMAKE_BINARY_DIR}/test_groups.json")
            if(NES_SYSTEM_TEST_VERBOSE)
                set(VERBOSE "--verbose")
            else()
                set(VERBOSE "")
            endif()

            # Create test targets
            file(READ "${CMAKE_BINARY_DIR}/tests.json" TESTS_JSON)

            string(JSON TESTS_COUNT LENGTH ${TESTS_JSON})
            math(EXPR TESTS_COUNT_END "${TESTS_COUNT} - 1")
            foreach(INDEX RANGE 0 ${TESTS_COUNT_END})
                string(JSON TEST_NAME GET ${TESTS_JSON} ${INDEX})
                get_filename_component(filename ${TEST_NAME} NAME_WE)
                message(STATUS "Found test ${filename}")
                if (NES_ENABLE_LLVM_LIT)
                    add_custom_target(
                            LIT_${filename}
                            USES_TERMINAL
                            COMMAND ${VENV_DIR}/bin/lit --filter ${filename} ${VERBOSE} ${CMAKE_SOURCE_DIR}/test -DCLIENT_BINARY=$<TARGET_FILE:nebuli> -DWORKER_BINARY=$<TARGET_FILE:single-node>
                            DEPENDS $<TARGET_FILE:nebuli> $<TARGET_FILE:single-node>
                            COMMENT "Running system level test ${filename}"
                    )
                endif()
                add_system_test(${TEST_NAME} ${filename})
            endforeach()

            # Create test group targets
            file(READ "${CMAKE_BINARY_DIR}/test_groups.json" TEST_GROUPS_JSON)

            string(JSON GROUP_COUNT LENGTH ${TEST_GROUPS_JSON})
            math(EXPR GROUP_COUNT_END "${GROUP_COUNT} - 1")
            foreach(INDEX RANGE 0 ${GROUP_COUNT_END})
                string(JSON GROUP_NAME MEMBER ${TEST_GROUPS_JSON} ${INDEX})
                string(JSON GROUP_TESTS GET ${TEST_GROUPS_JSON} ${GROUP_NAME})
                message(STATUS "Found test group ${GROUP_NAME}")
                string(JSON GROUP_TESTS_COUNT LENGTH ${GROUP_TESTS})
                math(EXPR GROUP_TESTS_COUNT_END "${GROUP_TESTS_COUNT} - 1")
                set(combined_filenames "")
                set(combined_filenames_local "")
                set(combined_test_names "")
                foreach(INDEX RANGE 0 ${GROUP_TESTS_COUNT_END})
                    string(JSON GROUP_TEST GET ${GROUP_TESTS} ${INDEX})
                    get_filename_component(filename ${GROUP_TEST} NAME_WE)
                    set(combined_test_names "${combined_test_names} ${GROUP_TEST}")
                    set(combined_filenames_local "${combined_filenames_local} ${filename}")

                    if (combined_filenames STREQUAL "")
                        set(combined_filenames "${filename}")
                    else()
                        set(combined_filenames "${combined_filenames}|${filename}")
                    endif()
                endforeach()

                # Add parentheses around the combined filenames
                set(combined_filenames "\"(${combined_filenames})\"")

                if (NES_ENABLE_LLVM_LIT)
                    add_custom_target("LITGRP_${GROUP_NAME}"
                            USES_TERMINAL
                            COMMAND ${VENV_DIR}/bin/lit --filter ${combined_filenames} ${VERBOSE} ${CMAKE_SOURCE_DIR}/test -DCLIENT_BINARY=$<TARGET_FILE:nebuli> -DWORKER_BINARY=$<TARGET_FILE:single-node>
                            DEPENDS $<TARGET_FILE:nebuli> $<TARGET_FILE:single-node>
                            COMMENT "Running system level tests in group ${GROUP_NAME}")
                endif()

                add_system_test_group(${GROUP_NAME} ${combined_test_names} ${combined_filenames_local})
            endforeach()

            # Move test/testdata and scripts/llvm-lit/result_checker.py to build directory
            file(COPY ${CMAKE_SOURCE_DIR}/test/testdata DESTINATION ${CMAKE_BINARY_DIR}/test)
            file(COPY ${CMAKE_SOURCE_DIR}/scripts/llvm-lit/result_checker.py DESTINATION ${CMAKE_BINARY_DIR}/test)

            message(STATUS "Enabled System Tests")
        else()
            message(NOTICE "Unable to create targets. Could not find ${CMAKE_BINARY_DIR}/tests.json or ${CMAKE_BINARY_DIR}/test_groups.json")
        endif()
    else()
        message(NOTICE "Disable System Tests. Could not find Python3.")
    endif()
endfunction()
