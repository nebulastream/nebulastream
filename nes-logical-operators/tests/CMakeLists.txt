# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function(add_nes_logical_operators_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-logical-operators)
endfunction()

add_nes_logical_operators_test(nes-logical-plan-test "LogicalPlanTest.cpp")
add_nes_logical_operators_test(store-logical-operator-test "StoreLogicalOperatorTest.cpp")

add_nes_test(nes-plan-recovery-compilation-cache-test "PlanRecoveryCompilationCacheTest.cpp")
target_link_libraries(
    nes-plan-recovery-compilation-cache-test
    nes-logical-operators
    nes-query-compiler
    nes-frontend-lib)
target_include_directories(
    nes-plan-recovery-compilation-cache-test
    PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/nes-query-compiler/private>)

add_nes_test(nes-multi-query-checkpoint-recovery-test "MultiQueryCheckpointRecoveryTest.cpp")
target_link_libraries(
    nes-multi-query-checkpoint-recovery-test
    nes-logical-operators
    nes-single-node-worker-lib
    nes-frontend-lib)
target_include_directories(
    nes-multi-query-checkpoint-recovery-test
    PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/nes-single-node-worker/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/nes-query-compiler/include>)
