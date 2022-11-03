
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_executable(nautilus-data-type-test "DataTypeTest.cpp")
target_link_libraries(nautilus-data-type-test
        nes-runtime ${GTEST_LIBRARIES})
add_test(NAME nautilus-data-type-test COMMAND nautilus-data-type-test)

add_executable(nautilus-custom-data-type-test "CustomDataTypeTest.cpp")
target_link_libraries(nautilus-custom-data-type-test
        nes-runtime ${GTEST_LIBRARIES})
add_test(NAME nautilus-custom-data-type-test COMMAND nautilus-custom-data-type-test)

add_executable(nautilus-list-type-test "ListTypeTest.cpp")
target_link_libraries(nautilus-list-type-test
        nes-runtime ${GTEST_LIBRARIES})
add_test(NAME nautilus-list-type-test COMMAND nautilus-list-type-test)

add_executable(nautilus-text-type-test "TextTypeTest.cpp")
target_link_libraries(nautilus-text-type-test
        nes-runtime ${GTEST_LIBRARIES})
add_test(NAME nautilus-text-type-test COMMAND nautilus-text-type-test)

