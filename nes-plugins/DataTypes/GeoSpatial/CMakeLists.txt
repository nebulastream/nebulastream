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
add_subdirectory(Functions)

add_library(geospatial_datatype_plugins STATIC CircleDataType.cpp LineDataType.cpp MovingPolygonDataType.cpp MovingPolygonTrajectoryDataType.cpp PointDataType.cpp PolygonDataType.cpp)
target_link_libraries(geospatial_datatype_plugins PRIVATE nes-data-types)
target_include_directories(geospatial_datatype_plugins PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

link_plugin_library(nes-data-types geospatial_datatype_plugins)
add_registry_entry(DataType Point)
add_registry_entry(DataType Line)
add_registry_entry(DataType Circle)
add_registry_entry(DataType Polygon)
add_registry_entry(DataType MovingPolygon)
add_registry_entry(DataType MovingPolygonTrajectory)
