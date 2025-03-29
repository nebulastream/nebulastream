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
        WindowAggregationFunction.cpp
)

add_plugin(Avg WindowAggregationFunction nes-logical-operators AvgAggregationFunction.cpp)
add_plugin(Count WindowAggregationFunction nes-logical-operators CountAggregationFunction.cpp)
add_plugin(Max WindowAggregationFunction nes-logical-operators MaxAggregationFunction.cpp)
add_plugin(Median WindowAggregationFunction nes-logical-operators MedianAggregationFunction.cpp)
add_plugin(Min WindowAggregationFunction nes-logical-operators MinAggregationFunction.cpp)
add_plugin(Sum WindowAggregationFunction nes-logical-operators SumAggregationFunction.cpp)
