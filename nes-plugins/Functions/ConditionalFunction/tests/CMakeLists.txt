# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_test(conditional-logical-function-test ConditionalLogicalFunctionTest.cpp)
target_link_libraries(conditional-logical-function-test nes-logical-operators nes-logical-operators-registry)
target_include_directories(conditional-logical-function-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)

add_nes_test(conditional-physical-function-test ConditionalPhysicalFunctionTest.cpp)
target_link_libraries(conditional-physical-function-test nes-data-types nes-physical-operators nes-memory-test-utils)
target_include_directories(conditional-physical-function-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
