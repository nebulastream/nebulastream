# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


add_subdirectory(TestUtils)

add_nes_unit_test(paged-vector-unit-tests "UnitTests/PagedVectorTest.cpp")
target_link_libraries(paged-vector-unit-tests nes-nautilus-test-util)

add_nes_unit_test(variable-sized-data-unit-tests "UnitTests/VariableSizedDataTest.cpp")
target_link_libraries(variable-sized-data-unit-tests nes-nautilus-test-util)

add_nes_unit_test(var-val-unit-tests "UnitTests/VarValTest.cpp")
target_link_libraries(var-val-unit-tests nes-nautilus-test-util)

add_nes_unit_test(chained-hashmap-unit-tests "UnitTests/ChainedHashMapTest.cpp")
target_link_libraries(chained-hashmap-unit-tests nes-nautilus-test-util)
