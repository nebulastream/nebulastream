# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

register_entry(Avg AggregationPhysicalFunction AvgAggregationPhysicalFunction.cpp)
register_entry(Count AggregationPhysicalFunction CountAggregationPhysicalFunction.cpp)
register_entry(Max AggregationPhysicalFunction MaxAggregationPhysicalFunction.cpp)
register_entry(Min AggregationPhysicalFunction MinAggregationPhysicalFunction.cpp)
register_entry(Median AggregationPhysicalFunction MedianAggregationPhysicalFunction.cpp)
register_entry(Sum AggregationPhysicalFunction SumAggregationPhysicalFunction.cpp)

add_source_files(nes-physical-operators
        AggregationPhysicalFunction.cpp
)
