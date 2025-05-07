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
        LogicalOperator.cpp
)

add_subdirectory(Sinks)
add_subdirectory(Sources)
add_subdirectory(Windows)

add_plugin(Selection LogicalOperator nes-logical-operators SelectionLogicalOperator.cpp)
add_plugin(Projection LogicalOperator nes-logical-operators ProjectionLogicalOperator.cpp)
add_plugin(Map LogicalOperator nes-logical-operators MapLogicalOperator.cpp)
add_plugin(Union LogicalOperator nes-logical-operators UnionLogicalOperator.cpp)
add_plugin(Sequence LogicalOperator nes-logical-operators SequenceLogicalOperator.cpp)
add_plugin(IngestionTimeWatermarkAssigner LogicalOperator nes-logical-operators IngestionTimeWatermarkAssignerLogicalOperator.cpp)
add_plugin(EventTimeWatermarkAssigner LogicalOperator nes-logical-operators EventTimeWatermarkAssignerLogicalOperator.cpp)
