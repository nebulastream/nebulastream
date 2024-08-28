# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# In this function we create target for system tests. We make heavy use of cache files to speed up the tests when
# running them multiple times. The cache files are created by the nebuli tool. The cache files are created in the
# build directory in the cache folder. After the initial creation of the cache files they are only updated if the
# system test file, nebuli or the single-node-worker binary changes.
function(add_system_test SYSTEM_TEST_FILE_PATH SYSTEM_TEST_NAME)
        # Name of the test executable
        set(TEST_EXECUTABLE "ST_${SYSTEM_TEST_NAME}")
        # Source file to generate gtests programmatically
        set(SOURCE_FILE "${CMAKE_SOURCE_DIR}/test/utils/SQLLogicTest.cpp")
        # Directory into which protobuf files are generated into
        set(CACHE_DIR "${CMAKE_BINARY_DIR}/cache/")
        # Nebuli generates cache files which are stored as "${CACHE_DIR}/${SYSTEM_TEST_NAME}_<test-number-in-file>.pb".
        # We use the variable CACHE_FILE to track if the newest cache files have been generated. As at cmake
        # configuration time we do not know how many tests a .test files we use only the first one.
        set(CACHE_FILE "${CACHE_DIR}/${SYSTEM_TEST_NAME}_0.pb")

        file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/test/result)

        add_custom_command(
                OUTPUT ${CACHE_FILE}
                COMMAND nebuli dump --input ${SYSTEM_TEST_FILE_PATH} --output ${CACHE_DIR}
                DEPENDS nebuli nes-single-node-worker ${SYSTEM_TEST_FILE_PATH}
        )

        add_executable(${TEST_EXECUTABLE} ${SOURCE_FILE} ${CACHE_FILE})
        target_link_libraries(${TEST_EXECUTABLE}
                PRIVATE
                nes-execution-test-util
                nebuli-lib
                nes-single-node-worker-test-util
        )

        target_compile_definitions(${TEST_EXECUTABLE}
                PRIVATE
                CMAKE_BINARY_DIR="${CMAKE_BINARY_DIR}"
                CACHE_DIR="${CACHE_DIR}"
                SYSTEM_TEST_NAME="${SYSTEM_TEST_NAME}"
                SYSTEM_TEST_FILE_PATH="${SYSTEM_TEST_FILE_PATH}"
        )

        gtest_discover_tests(${TEST_EXECUTABLE}
                WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endfunction()


function(add_system_test_group GROUP_NAME SYSTEM_TEST_FILE_PATHS SYSTEM_TEST_NAMES)
        # Name of the test executable
        set(TEST_EXECUTABLE "STGRP_${GROUP_NAME}")
        # Source file to generate gtests programmatically
        set(SOURCE_FILE "${CMAKE_SOURCE_DIR}/test/utils/SQLLogicTest.cpp")
        # Directory into which protobuf files are generated into
        set(CACHE_DIR "${CMAKE_BINARY_DIR}/cache/")

        file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/test/result)

        string(REGEX MATCHALL "[^ ]+" SYSTEM_TEST_FILE_PATHS_ "${SYSTEM_TEST_FILE_PATHS}")
        set(CACHE_FILES)
        foreach (SYSTEM_TEST_FILE_PATH IN LISTS SYSTEM_TEST_FILE_PATHS_)
                get_filename_component(SYSTEM_TEST_NAME ${SYSTEM_TEST_FILE_PATH} NAME_WE)
                set(CACHE_FILE "${CACHE_DIR}/${SYSTEM_TEST_NAME}_0.pb")
                list(APPEND CACHE_FILES ${CACHE_FILE})
        endforeach()

        # Here we assume that the `add_custom_command` rules for creating the cache files have already been created
        # while creating the single test targets
        add_executable(${TEST_EXECUTABLE} EXCLUDE_FROM_ALL ${SOURCE_FILE} ${CACHE_FILES})
        target_link_libraries(${TEST_EXECUTABLE}
                PRIVATE
                nes-execution-test-util
                nebuli-lib
                nes-single-node-worker-test-util
        )

        target_compile_definitions(${TEST_EXECUTABLE}
                PRIVATE
                CMAKE_BINARY_DIR="${CMAKE_BINARY_DIR}"
                CACHE_DIR="${CACHE_DIR}"
                SYSTEM_TEST_NAME="${SYSTEM_TEST_NAMES}"
                SYSTEM_TEST_FILE_PATH="${SYSTEM_TEST_FILE_PATHS}"
        )

        gtest_discover_tests(${TEST_EXECUTABLE}
                WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endfunction()
