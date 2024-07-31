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
# build directory in the cache folder. After the initial creation of the cache files the are only updated if the
# system test file changes.

# TODO: add option to run locally tests by groups
function(add_system_test_file SYSTEM_TEST_FILE_PATH SYSTEM_TEST_NAME)
    # Check if a system cache file exists and if not create it. In general more than one cache file is created _0, _1, _2, ...
    # but it is enough to check if the first one exists.
    if (NOT EXISTS "${CMAKE_BINARY_DIR}/nes-nebuli/nebuli")
        message(ERROR "nes-nebuli not found. Please build the project once and reload the cmake to create system test targets.")
    endif()
    set(set RESULT 0)
    file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/cache/)
    file(GLOB CACHE_FILE "${CMAKE_BINARY_DIR}/cache/${SYSTEM_TEST_NAME}_0.pb")
    if (NOT EXISTS ${CACHE_FILE})
        message(STATUS "Creating cache file for ${SYSTEM_TEST_NAME}")
        execute_process(
                COMMAND ${CMAKE_BINARY_DIR}/nes-nebuli/nebuli dump --input ${SYSTEM_TEST_FILE_PATH} --output ${CMAKE_BINARY_DIR}/cache/
                RESULT_VARIABLE RESULT
        )
    endif()
    if (RESULT EQUAL 0)
        file(GLOB CACHE_FILES "${CMAKE_BINARY_DIR}/cache/${SYSTEM_TEST_NAME}_*")

        list(LENGTH CACHE_FILES CACHE_FILES_COUNT)
        math(EXPR CACHE_FILES_COUNT_END "${CACHE_FILES_COUNT} - 1")
        foreach(INDEX RANGE 0 ${CACHE_FILES_COUNT_END})
            list(GET CACHE_FILES ${INDEX} CACHE_FILE)
            get_filename_component(CACHE_FILE_NAME ${CACHE_FILE} NAME_WE)
            if (${INDEX} EQUAL 0)
                set(SQLLogicTestParam "QueryTestParam{\"${CACHE_FILE}\", \"${SYSTEM_TEST_FILE_PATH}\", ${INDEX}}")
            else()
                set(SQLLogicTestParam "${SQLLogicTestParam}, QueryTestParam{\"${CACHE_FILE}\", \"${SYSTEM_TEST_FILE_PATH}\", ${INDEX}}")
            endif()
        endforeach()

        message(STATUS "Creating test target for ${SYSTEM_TEST_NAME}")
        add_executable(ST_${SYSTEM_TEST_NAME} ${CMAKE_SOURCE_DIR}/test/utils/SQLLogicTest.cpp "${CMAKE_BINARY_DIR}/cache/${SYSTEM_TEST_NAME}_0.pb")
        gtest_discover_tests(ST_${SYSTEM_TEST_NAME}
                WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                DISCOVERY_MODE PRE_TEST
                DISCOVERY_TIMEOUT 30
                PROPERTIES ENVIRONMENT SQLLogicTest_PLACEHOLDER=${SQLLogicTestParam}
                PROPERTIES ENVIRONMENT SYSTEM_TEST_NAME="${SYSTEM_TEST_NAME}"
                PROPERTIES ENVIRONMENT CMAKE_BINARY_DIR="${CMAKE_BINARY_DIR}"
        )
        target_compile_definitions(ST_${SYSTEM_TEST_NAME} PRIVATE SQLLogicTest_PLACEHOLDER=${SQLLogicTestParam})
        target_compile_definitions(ST_${SYSTEM_TEST_NAME} PRIVATE SYSTEM_TEST_NAME="${SYSTEM_TEST_NAME}")
        target_compile_definitions(ST_${SYSTEM_TEST_NAME} PRIVATE CMAKE_BINARY_DIR="${CMAKE_BINARY_DIR}")

        target_link_libraries(ST_${SYSTEM_TEST_NAME} nes-test-util)
        target_link_libraries(ST_${SYSTEM_TEST_NAME} nes-single-node-worker-test-util nebuli-lib)

        add_custom_command(
                OUTPUT "${CMAKE_BINARY_DIR}/cache/${SYSTEM_TEST_NAME}_0.pb"
                COMMAND nebuli dump --input ${SYSTEM_TEST_FILE_PATH} --output ${CMAKE_BINARY_DIR}/cache/
                DEPENDS nebuli ${SYSTEM_TEST_FILE_PATH})

        file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/test/result/)
    endif()
endfunction()

