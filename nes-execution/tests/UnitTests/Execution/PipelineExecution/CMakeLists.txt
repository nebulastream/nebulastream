# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
add_nes_execution_test(execution-scan-emit-pipeline-test "ScanEmitPipelineTest.cpp")
add_nes_execution_test(execution-selection-pipeline-test "SelectionPipelineTest.cpp")
add_nes_execution_test(execution-sequence-number-pipeline-test "SequenceNumberPipelineTest.cpp")
add_nes_execution_test(execution-nonkeyed-window-pipeline-test "NonKeyedTimeWindowPipelineTest.cpp")
add_nes_execution_test(execution-keyed-window-pipeline-test "KeyedTimeWindowPipelineTest.cpp")
add_nes_execution_test(execution-text-pipeline-test "TextPipelineTest.cpp")
add_nes_execution_test(execution-stream-hash-join-pipeline-test "StreamHashJoinPipelineTest.cpp")
add_nes_execution_test(execution-stream-nested-loop-join-pipeline-test "NestedLoopJoinPipelineTest.cpp")
