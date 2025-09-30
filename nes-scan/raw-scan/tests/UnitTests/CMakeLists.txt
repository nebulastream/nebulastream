# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_definitions(-DINPUT_FORMATTER_TEST_DATA="${CMAKE_CURRENT_SOURCE_DIR}/testdata")
set(TEMP_RESULTS_PATH "${CMAKE_CURRENT_BINARY_DIR}/results")
add_definitions(-DINPUT_FORMATTER_TMP_RESULT_DATA="${TEMP_RESULTS_PATH}")
file(MAKE_DIRECTORY ${TEMP_RESULTS_PATH})

add_nes_input_formatter_test(input-formatter-test-specific-sequence "SpecificSequenceTest.cpp")
add_nes_input_formatter_test(input-formatter-test-small-files "SmallFilesTest.cpp")
add_nes_input_formatter_test(input-formatter-test-concurrent-synchronization "ConcurrentSynchronizationTest.cpp")
