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

# Logical FUnction library
add_library(meos_logical_functions STATIC
        NearestApproachDistanceLogicalFunction.cpp)
target_link_libraries(meos_logical_functions PRIVATE nes-logical-operators)
target_include_directories(meos_logical_functions PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Physical Function library
add_library(meos_physical_functions STATIC
        TemporalPointNearestApproachDistancePhysicalFunction.cpp)
target_link_libraries(meos_physical_functions PRIVATE nes-physical-operators)
target_link_libraries(meos_physical_functions PUBLIC nes-meos)
target_include_directories(meos_physical_functions PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Logical Function registrations
link_plugin_library(nes-logical-operators meos_logical_functions)
add_registry_entry(LogicalFunction NearestApproachDistance KEY NEAREST_APPROACH_DISTANCE)

# Physical Function registrations
link_plugin_library(nes-physical-operators meos_physical_functions)
add_registry_entry(PhysicalFunction TemporalPointNearestApproachDistance KEY NEAREST_APPROACH_DISTANCE_TemporalPoint_TemporalPoint)

# Unreflection registrations
add_unreflection_entry(LogicalFunction NearestApproachDistance KEY NEAREST_APPROACH_DISTANCE)
