# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin(Avg AggregationPhysicalFunction nes-physical-operators AvgAggregationPhysicalFunction.cpp)
add_plugin(Count AggregationPhysicalFunction nes-physical-operators CountAggregationPhysicalFunction.cpp)
add_plugin(Max AggregationPhysicalFunction nes-physical-operators MaxAggregationPhysicalFunction.cpp)
add_plugin(Min AggregationPhysicalFunction nes-physical-operators MinAggregationPhysicalFunction.cpp)
add_plugin(Median AggregationPhysicalFunction nes-physical-operators MedianAggregationPhysicalFunction.cpp)
add_plugin(Sum AggregationPhysicalFunction nes-physical-operators SumAggregationPhysicalFunction.cpp)
add_plugin(Array_Agg AggregationPhysicalFunction nes-physical-operators ArrayAggregationPhysicalFunction.cpp)

add_source_files(nes-physical-operators
        AggregationPhysicalFunction.cpp
)
