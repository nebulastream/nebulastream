
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_executable(nautilus-remove-br-only-blocks-phase-test "RemoveBranchOnlyBlocksPhaseTest.cpp")
target_link_libraries(nautilus-remove-br-only-blocks-phase-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME nautilus-remove-br-only-blocks-phase-test COMMAND nautilus-remove-br-only-blocks-phase-test)

add_executable(nautilus-structured-control-flow-phase-test "StructuredControlFlowPhaseTest.cpp")
target_link_libraries(nautilus-structured-control-flow-phase-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME nautilus-structured-control-flow-phase-test COMMAND nautilus-structured-control-flow-phase-test)