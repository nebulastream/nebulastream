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

# Property-based ChainedHashMap tests using RapidCheck.
# Replaces the old parameterized chained-hashmap-unit-tests and chained-hashmap-unit-tests-custom-value.
find_package(rapidcheck CONFIG REQUIRED)
set(RAPIDCHECK_MAX_SUCCESS_FAST "10" CACHE STRING "Number of RapidCheck iterations for fast property tests")
set(RAPIDCHECK_MAX_SUCCESS_SLOW "100" CACHE STRING "Number of RapidCheck iterations for slow property tests")

add_executable(chained-hashmap-property-tests "UnitTests/ChainedHashMapPropertyTest.cpp")
target_link_libraries(chained-hashmap-property-tests nes-test-util nes-data-types nes-nautilus-test-util rapidcheck rapidcheck_gtest)

# Fast: fewer iterations, suitable for CI
add_test(NAME chained-hashmap-property-tests-fast
    COMMAND ${CMAKE_COMMAND} -E env "RC_PARAMS=max_success=${RAPIDCHECK_MAX_SUCCESS_FAST}" $<TARGET_FILE:chained-hashmap-property-tests>)
set_tests_properties(chained-hashmap-property-tests-fast PROPERTIES LABELS "fast" TIMEOUT 120)

# Slow: more iterations, for thorough testing
add_test(NAME chained-hashmap-property-tests-slow
    COMMAND ${CMAKE_COMMAND} -E env "RC_PARAMS=max_success=${RAPIDCHECK_MAX_SUCCESS_SLOW}" $<TARGET_FILE:chained-hashmap-property-tests>)
set_tests_properties(chained-hashmap-property-tests-slow PROPERTIES LABELS "slow" TIMEOUT 600)

# Property-based PagedVector tests using RapidCheck.
add_executable(paged-vector-property-tests "UnitTests/PagedVectorPropertyTest.cpp")
target_link_libraries(paged-vector-property-tests nes-test-util nes-data-types nes-nautilus-test-util rapidcheck rapidcheck_gtest)

add_test(NAME paged-vector-property-tests-fast
    COMMAND ${CMAKE_COMMAND} -E env "RC_PARAMS=max_success=${RAPIDCHECK_MAX_SUCCESS_FAST}" $<TARGET_FILE:paged-vector-property-tests>)
set_tests_properties(paged-vector-property-tests-fast PROPERTIES LABELS "fast" TIMEOUT 120)

add_test(NAME paged-vector-property-tests-slow
    COMMAND ${CMAKE_COMMAND} -E env "RC_PARAMS=max_success=${RAPIDCHECK_MAX_SUCCESS_SLOW}" $<TARGET_FILE:paged-vector-property-tests>)
set_tests_properties(paged-vector-property-tests-slow PROPERTIES LABELS "slow" TIMEOUT 600)
