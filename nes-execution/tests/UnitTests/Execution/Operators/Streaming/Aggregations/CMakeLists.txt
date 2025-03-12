# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_execution_test(execution-global-slice-pre-aggregation-test "NonKeyedSlicePreAggregationTest.cpp")
add_nes_execution_test(execution-keyed-slice-pre-aggregation-test "KeyedSlicePreAggregationTest.cpp")
add_nes_execution_test(execution-keyed-serialization-test "KeyedSliceSerializationTest.cpp")
add_nes_execution_test(execution-multi-origin-watermark-processor-test "MultiOriginWatermarkProcessorTest.cpp")
add_nes_execution_test(execution-slice-assigner-test "SliceAssignerTest.cpp")

add_nes_execution_test(execution-append-to-slicestore-action-test "AppendToSliceStoreActionTest.cpp")
add_nes_execution_test(execution-keyed-slice-merging-test "KeyedSliceMergingTest.cpp")
