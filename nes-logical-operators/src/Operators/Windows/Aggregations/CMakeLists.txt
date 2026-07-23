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

add_registry_entry(Avg AggregationLogicalFunction AvgAggregationLogicalFunction.cpp)
add_registry_entry(Count AggregationLogicalFunction CountAggregationLogicalFunction.cpp)
add_registry_entry(Max AggregationLogicalFunction MaxAggregationLogicalFunction.cpp)
add_registry_entry(Median AggregationLogicalFunction MedianAggregationLogicalFunction.cpp)
add_registry_entry(Min AggregationLogicalFunction MinAggregationLogicalFunction.cpp)
add_registry_entry(Sum AggregationLogicalFunction SumAggregationLogicalFunction.cpp)

add_unreflection_entry(AggregationLogicalFunction Avg)
add_unreflection_entry(AggregationLogicalFunction Count)
add_unreflection_entry(AggregationLogicalFunction Max)
add_unreflection_entry(AggregationLogicalFunction Median)
add_unreflection_entry(AggregationLogicalFunction Min)
add_unreflection_entry(AggregationLogicalFunction Sum)
