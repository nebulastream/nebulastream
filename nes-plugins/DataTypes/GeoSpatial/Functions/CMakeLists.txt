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

add_library(geospatial_logical_functions STATIC CircleAreaLogicalFunction.cpp CircleCircumferenceLogicalFunction.cpp PointCircleContainsLogicalFunction.cpp PointLineContainsLogicalFunction.cpp)
target_link_libraries(geospatial_logical_functions PRIVATE nes-logical-operators)
target_include_directories(geospatial_logical_functions PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_library(geospatial_physical_functions STATIC CircleAreaPhysicalFunction.cpp CirclePointAddPhysicalFunction.cpp PointAddPhysicalFunction.cpp CircleCircumferencePhysicalFunction.cpp PointCircleContainsPhysicalFunction.cpp PointLineContainsPhysicalFunction.cpp)
target_link_libraries(geospatial_physical_functions PRIVATE nes-physical-operators)
target_include_directories(geospatial_physical_functions PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

link_plugin_library(nes-logical-operators geospatial_logical_functions)
add_registry_entry(LogicalFunction CircleArea KEY CIRCLE_AREA)
add_registry_entry(LogicalFunction CircleCircumference KEY CIRCLE_CIRCUMFERENCE)
add_registry_entry(LogicalFunction PointCircleContains KEY POINT_CIRCLE_CONTAINS)
add_registry_entry(LogicalFunction PointLineContains KEY POINT_LINE_CONTAINS)

link_plugin_library(nes-physical-operators geospatial_physical_functions)
add_registry_entry(PhysicalFunction PointAdd KEY Add_Point_Point)
add_registry_entry(PhysicalFunction CirclePointAdd KEY Add_Circle_Point)
add_registry_entry(PhysicalFunction CircleArea KEY CIRCLE_AREA)
add_registry_entry(PhysicalFunction CircleCircumference KEY CIRCLE_CIRCUMFERENCE)
add_registry_entry(PhysicalFunction PointCircleContains KEY POINT_CIRCLE_CONTAINS)
add_registry_entry(PhysicalFunction PointLineContains KEY POINT_LINE_CONTAINS)

add_unreflection_entry(LogicalFunction CircleArea KEY CIRCLE_AREA)
add_unreflection_entry(LogicalFunction CircleCircumference KEY CIRCLE_CIRCUMFERENCE)
add_unreflection_entry(LogicalFunction PointCircleContains KEY POINT_CIRCLE_CONTAINS)
add_unreflection_entry(LogicalFunction PointLineContains KEY POINT_LINE_CONTAINS)
