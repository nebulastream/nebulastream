
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_runtime_test(runtime-scan-emit-pipeline-test "ScanEmitPipelineTest.cpp")
add_nes_runtime_test(runtime-selection-pipeline-test "SelectionPipelineTest.cpp")
add_nes_runtime_test(runtime-threshold-window-pipeline-test "ThresholdWindowPipelineTest.cpp")
add_nes_runtime_test(runtime-text-pipeline-test "SelectionPipelineTest.cpp")
add_nes_runtime_test(runtime-lazy-join-pipeline-test "StreamJoinPipelineTest.cpp")

add_executable(runtime-threshold-window-pipeline-test "ThresholdWindowPipelineTest.cpp")
target_link_libraries(runtime-threshold-window-pipeline-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-threshold-window-pipeline-test COMMAND runtime-threshold-window-pipeline-test)

add_executable(runtime-global-window-pipeline-test "GlobalTimeWindowPipelineTest.cpp")
target_link_libraries(runtime-global-window-pipeline-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-global-window-pipeline-test COMMAND runtime-global-window-pipeline-test)

add_executable(runtime-text-pipeline-test "TextPipelineTest.cpp")
target_link_libraries(runtime-text-pipeline-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-text-pipeline-test COMMAND runtime-text-pipeline-test)