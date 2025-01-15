# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


add_library(nes-sources-test-utils Util/TestSource.cpp)
target_include_directories(nes-sources-test-utils PUBLIC ../private Util)
target_link_libraries(nes-sources-test-utils PUBLIC nes-sources nes-memory-test-utils)

function(add_nes_source_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-memory-test-utils nes-sources-test-utils)
endfunction()

add_nes_source_test(blocking-source-runner-test Unit/BlockingSourceRunnerTest.cpp)


add_nes_unit_test(source-unit-tests
        "Unit/FileSourceTest.cpp"
        "Unit/TCPSourceTest.cpp"
)

target_include_directories(source-unit-tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../private)

target_link_libraries(source-unit-tests nes-sources)
