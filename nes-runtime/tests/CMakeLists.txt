
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(ExternalProject)
add_subdirectory(Util)

# Copy Test Data
file(COPY testData DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
add_compile_definitions(TEST_DATA_DIRECTORY="${CMAKE_CURRENT_BINARY_DIR}/testData/")
message(STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}/testData")


include(FetchContent)
FetchContent_Declare(tpch-dbgen
        GIT_REPOSITORY https://github.com/nebulastream/tpch-dbgen.git
        GIT_TAG master
        )

FetchContent_MakeAvailable(tpch-dbgen)
# The tpch-dbgen target causes warnings on recent compilers. Thus we disable the warning for this target.
target_compile_options(tpch-dbgen PUBLIC "-Wno-deprecated-non-prototype")

function(add_nes_runtime_test)
    add_executable(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} PRIVATE nes-runtime-test-util)
    if (NES_ENABLE_PRECOMPILED_HEADERS)
        target_precompile_headers(${TARGET_NAME} REUSE_FROM nes-common)
    endif ()
    if (CODE_COVERAGE)
        target_code_coverage(${TARGET_NAME} PUBLIC AUTO ALL EXTERNAL OBJECTS nes-runtime-test-util EXCLUDE tests/*)
        message(STATUS "Added cc test ${TARGET_NAME}")
    endif ()
    gtest_add_tests(TARGET ${TARGET_NAME})
    message(STATUS "Added unit test ${TARGET_NAME}")
endfunction()

add_subdirectory(UnitTests)