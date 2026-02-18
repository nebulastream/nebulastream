# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function(add_nes_optimizer_test)
    add_nes_unit_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-query-optimizer nes-logical-operators nes-test-util)
    target_include_directories(${TARGET_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../private)
endfunction()

add_nes_optimizer_test(DecideJoinTypesTest UnitTests/DecideJoinTypesTest.cpp)
add_nes_optimizer_test(DecideMemoryLayoutTest UnitTests/DecideMemoryLayoutTest.cpp)
add_nes_optimizer_test(PredicatePushdownTest UnitTests/PredicatePushdownTest.cpp)
add_nes_optimizer_test(ProjectionPruningTest UnitTests/ProjectionPruningTest.cpp)
