
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
include(GoogleTest)

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

function(add_nes_execution_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-execution-test-util nes-operators-test-util)
endfunction()

add_subdirectory(UnitTests)
add_subdirectory(src)

message(STATUS "Test CMAKE_CURRENT_SOURCE_DIR Directory: ${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(nes-execution-test-util PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_link_libraries(nes-execution-test-util PUBLIC nes-execution nes-common nes-data-types nes-operators nes-grpc nes-runtime nes-test-util nes-configurations nes-nautilus)
target_compile_definitions(nes-execution-test-util INTERFACE JAVA_UDF_TEST_DATA="${CMAKE_CURRENT_SOURCE_DIR}/testData/JavaUDFTestData")