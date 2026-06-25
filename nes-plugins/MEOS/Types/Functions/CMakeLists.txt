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

add_plugin_as_library(NEAREST_APPROACH_DISTANCE LogicalFunction nes-logical-operators-registry nearest_approach_distance_logical_function NearestApproachDistanceLogicalFunction.cpp)
add_plugin_as_library(NEAREST_APPROACH_DISTANCE PhysicalFunction nes-physical-operators-registry nearest_approach_distance_physical_function NearestApproachDistancePhysicalFunction.cpp)


target_include_directories(nearest_approach_distance_logical_function
        PUBLIC include
        PRIVATE .
)
target_include_directories(nearest_approach_distance_physical_function
        PUBLIC include
        PRIVATE .
)
target_link_libraries(nearest_approach_distance_physical_function PUBLIC nes-meos)



