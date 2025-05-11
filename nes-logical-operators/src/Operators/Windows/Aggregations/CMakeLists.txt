# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-logical-operators
        WindowAggregationLogicalFunction.cpp
)

add_plugin(Avg AggregationLogicalFunction nes-logical-operators AvgAggregationLogicalFunction.cpp)
add_plugin(Count AggregationLogicalFunction nes-logical-operators CountAggregationLogicalFunction.cpp)
add_plugin(Max AggregationLogicalFunction nes-logical-operators MaxAggregationLogicalFunction.cpp)
add_plugin(Median AggregationLogicalFunction nes-logical-operators MedianAggregationLogicalFunction.cpp)
add_plugin(Min AggregationLogicalFunction nes-logical-operators MinAggregationLogicalFunction.cpp)
add_plugin(Sum AggregationLogicalFunction nes-logical-operators SumAggregationLogicalFunction.cpp)
add_plugin(Array_Agg AggregationLogicalFunction nes-logical-operators ArrayAggregationLogicalFunction.cpp)
