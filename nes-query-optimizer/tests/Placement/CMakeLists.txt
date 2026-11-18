# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function(add_nes_placement_test)
    add_nes_optimizer_test(${ARGN})
    target_link_libraries(${ARGV0} nes-distributed-test-cluster-lib)
    target_include_directories(${ARGV0} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
endfunction()

add_nes_placement_test(query-decomposition-test QueryDecompositionTest.cpp)
add_nes_placement_test(global-execution-plan-test GlobalExecutionPlanTest.cpp)
add_nes_placement_test(incremental-placement-utils-test IncrementalPlacementUtilsTest.cpp)
add_nes_placement_test(incremental-bottom-up-placement-test IncrementalBottomUpPlacementTest.cpp)
add_nes_placement_test(placement-removal-strategy-test PlacementRemovalStrategyTest.cpp)
add_nes_placement_test(placement-addition-strategy-test PlacementAdditionStrategyTest.cpp)
add_nes_placement_test(query-placement-amendment-phase-test QueryPlacementAmendmentPhaseTest.cpp)
