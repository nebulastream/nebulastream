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

add_registry_entry(Selection LogicalOperator SelectionLogicalOperator.cpp)
add_registry_entry(Projection LogicalOperator ProjectionLogicalOperator.cpp)
add_registry_entry(Union LogicalOperator UnionLogicalOperator.cpp)
add_registry_entry(IngestionTimeWatermarkAssigner LogicalOperator IngestionTimeWatermarkAssignerLogicalOperator.cpp)
add_registry_entry(EventTimeWatermarkAssigner LogicalOperator EventTimeWatermarkAssignerLogicalOperator.cpp)
add_registry_entry(InferModel LogicalOperator InferModelLogicalOperator.cpp)
add_registry_entry(InferModelName LogicalOperator InferModelNameLogicalOperator.cpp)

add_unreflection_entry(LogicalOperator Selection)
add_unreflection_entry(LogicalOperator Projection)
add_unreflection_entry(LogicalOperator Union)
add_unreflection_entry(LogicalOperator IngestionTimeWatermarkAssigner)
add_unreflection_entry(LogicalOperator EventTimeWatermarkAssigner)
add_unreflection_entry(LogicalOperator InferModel)
add_unreflection_entry(LogicalOperator InferModelName)
