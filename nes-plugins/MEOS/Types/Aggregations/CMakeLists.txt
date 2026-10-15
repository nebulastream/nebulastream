# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)
include(${PROJECT_SOURCE_DIR}/cmake/UnreflectionRegistrationUtil.cmake)

# Logical Aggregation Library
add_library(meos_logical_aggregations STATIC
        TemporalSequenceAggregationLogicalFunction.cpp)
target_link_libraries(meos_logical_aggregations PRIVATE nes-logical-operators)
target_include_directories(meos_logical_aggregations PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Physical Aggregation library
add_library(meos_physical_aggregations STATIC
        TemporalSequenceAggregationPhysicalFunction.cpp)
target_link_libraries(meos_physical_aggregations PRIVATE nes-physical-operators)
target_link_libraries(meos_physical_aggregations PUBLIC nes-meos)
target_include_directories(meos_physical_aggregations PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Logical Aggregation registrations
link_plugin_library(nes-logical-operators meos_logical_aggregations)
add_registry_entry(AggregationLogicalFunction TemporalSequence KEY TEMPORAL_SEQUENCE)

# Physical Aggregation registrations
link_plugin_library(nes-physical-operators meos_physical_aggregations)
add_registry_entry(AggregationPhysicalFunction TemporalSequence KEY TEMPORAL_SEQUENCE)

# Unreflection registrations
add_unreflection_entry(AggregationLogicalFunction TemporalSequence KEY TEMPORAL_SEQUENCE)
