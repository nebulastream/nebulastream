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

add_plugin_as_library(Add_Point_Point PhysicalFunction point_add_physical_function PointAddPhysicalFunction.cpp)
add_plugin_as_library(Add_Circle_Point PhysicalFunction circle_point_add_physical_function CirclePointAddPhysicalFunction.cpp)
add_plugin_as_library(CIRCLE_AREA LogicalFunction circle_area_logical_function CircleAreaLogicalFunction.cpp)
add_plugin_as_library(CIRCLE_AREA PhysicalFunction circle_area_physical_function CircleAreaPhysicalFunction.cpp)

target_include_directories(point_add_physical_function PRIVATE .)

target_include_directories(circle_point_add_physical_function PRIVATE .)

target_include_directories(circle_area_logical_function PRIVATE .)

target_include_directories(circle_area_physical_function PRIVATE .)

add_unreflection_plugin(LogicalFunction CircleArea KEY CIRCLE_AREA)