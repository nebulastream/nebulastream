
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
add_nes_runtime_test(runtime-nonkeyed-threshold-window-pipeline-test "NonKeyedThresholdWindowPipelineTest.cpp")
add_nes_runtime_test(runtime-keyed-threshold-window-pipeline-test "KeyedThresholdWindowPipelineTest.cpp")
add_nes_runtime_test(runtime-nonkeyed-window-pipeline-test "NonKeyedTimeWindowPipelineTest.cpp")
add_nes_runtime_test(runtime-keyed-window-pipeline-test "KeyedTimeWindowPipelineTest.cpp")
add_nes_runtime_test(runtime-text-pipeline-test "TextPipelineTest.cpp")
add_nes_runtime_test(runtime-stream-hash-join-pipeline-test "StreamHashJoinPipelineTest.cpp")
add_nes_runtime_test(runtime-stream-nested-loop-join-pipeline-test "NestedLoopJoinPipelineTest.cpp")
add_nes_runtime_test(runtime-map-pythonudf-pipeline-test "MapPythonUDFPipelineTest.cpp")
add_nes_runtime_test(runtime-batch-aggregation-pipeline-test "BatchAggregationPipelineTest.cpp")
add_nes_runtime_test(runtime-batch-join-pipeline-test "BatchJoinPipelineTest.cpp")
add_nes_runtime_test(runtime-limit-pipeline-test "LimitPipelineTest.cpp")

if (NES_ENABLE_EXPERIMENTAL_EXECUTION_JNI)
    add_nes_runtime_test(runtime-javaudf-pipeline-test "MapJavaUDFPipelineTest.cpp" "FlatMapJavaUDFPipelineTest.cpp")
endif ()