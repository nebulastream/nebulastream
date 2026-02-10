# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function(add_nes_test_systest)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-systest-lib)
    target_include_directories(${TARGET_NAME}
            PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
    target_compile_definitions(${TARGET_NAME} PRIVATE SYSTEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/testdata/")
endfunction()

add_nes_test_systest(slt-parser-test
        "SystestParserTests.cpp"
        "SystestParserInvalidTestFilesTests.cpp"
        "SystestParserValidTestFilesTests.cpp"
        "SystestRunnerTest.cpp")

add_nes_test_systest(slt-e2e-test
        "SystestE2ETests.cpp")
