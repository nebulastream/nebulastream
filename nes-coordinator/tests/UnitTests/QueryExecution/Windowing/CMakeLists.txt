# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_unit_test(nonkeyed-tumbling-window-query-execution-test "NonKeyedTumblingWindowQueryExecutionTest.cpp")
add_nes_unit_test(nonkeyed-tumbling-window-aggregation-function-test "WindowAggregationFunctionTest.cpp")
add_nes_unit_test(nonkeyed-sliding-window-aggregation-function-test "NonKeyedSlidingWindowQueryExecutionTest.cpp")
add_nes_unit_test(keyed-sliding-window-aggregation-function-test "KeyedSlidingWindowQueryExecutionTest.cpp")
add_nes_unit_test(keyed-tumbling-window-query-execution-test "KeyedTumblingWindowQueryExecutionTest.cpp")
add_nes_unit_test(threshold-window-query-execution-test "ThresholdWindowQueryExecutionTest.cpp")
