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

add_plugin_as_library(Add_Point_Point PhysicalFunction nes-physical-operators-registry point_add_physical_function PointAddPhysicalFunction.cpp)
add_plugin_as_library(Add_Circle_Point PhysicalFunction nes-physical-operators-registry circle_point_add_physical_function CirclePointAddPhysicalFunction.cpp)
add_plugin_as_library(CIRCLE_AREA LogicalFunction nes-logical-operators-registry circle_area_logical_function CircleAreaLogicalFunction.cpp)
add_plugin_as_library(CIRCLE_AREA PhysicalFunction nes-physical-operators-registry circle_area_physical_function CircleAreaPhysicalFunction.cpp)
add_plugin_as_library(CIRCLE_CIRCUMFERENCE LogicalFunction nes-logical-operators-registry circle_circumference_logical_function CircleCircumferenceLogicalFunction.cpp)
add_plugin_as_library(CIRCLE_CIRCUMFERENCE PhysicalFunction nes-physical-operators-registry circle_circumference_physical_function CircleCircumferencePhysicalFunction.cpp)
add_plugin_as_library(POINT_CIRCLE_CONTAINS LogicalFunction nes-logical-operators-registry point_circle_contains_logical_function PointCircleContainsLogicalFunction.cpp)
add_plugin_as_library(POINT_CIRCLE_CONTAINS PhysicalFunction nes-physical-operators-registry point_circle_contains_physical_function PointCircleContainsPhysicalFunction.cpp)
add_plugin_as_library(POINT_LINE_CONTAINS LogicalFunction nes-logical-operators-registry point_line_contains_logical_function PointLineContainsLogicalFunction.cpp)
add_plugin_as_library(POINT_LINE_CONTAINS PhysicalFunction nes-physical-operators-registry point_line_contains_physical_function PointLineContainsPhysicalFunction.cpp)


target_include_directories(point_add_physical_function
        PUBLIC include
        PRIVATE .
)

target_include_directories(circle_point_add_physical_function
        PUBLIC include
        PRIVATE .
)

target_include_directories(circle_area_logical_function
        PUBLIC include
        PRIVATE .
)

target_include_directories(circle_area_physical_function
        PUBLIC include
        PRIVATE .
)

target_include_directories(circle_circumference_logical_function
        PUBLIC include
        PRIVATE .
)

target_include_directories(circle_circumference_physical_function
        PUBLIC include
        PRIVATE .
)

target_include_directories(point_circle_contains_logical_function
        PUBLIC include
        PRIVATE .
)

target_include_directories(point_circle_contains_physical_function
        PUBLIC include
        PRIVATE .
)

target_include_directories(point_line_contains_logical_function
        PUBLIC include
        PRIVATE .
)

target_include_directories(point_line_contains_physical_function
        PUBLIC include
        PRIVATE .
)
