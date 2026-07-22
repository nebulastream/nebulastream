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
add_plugin_as_library(MovingPoint DataType nes-data-types-registry moving_point_data_type_plugin MovingPointDataType.cpp)
add_plugin_as_library(SpatioTemporalBox DataType nes-data-types-registry spatio_temporal_box_data_type_plugin SpatioTemporalBox.cpp)
add_plugin_as_library(Polygon DataType nes-data-types-registry polygon_data_type_plugin PolygonDataType.cpp)
add_plugin_as_library(MovingPolygon DataType nes-data-types-registry moving-polygon_data_type_plugin MovingPolygonDataType.cpp)

target_include_directories(moving_point_data_type_plugin
        PUBLIC include
        PRIVATE .
)
target_include_directories(spatio_temporal_box_data_type_plugin
        PUBLIC include
        PRIVATE .
)

target_include_directories(polygon_data_type_plugin
        PUBLIC include
        PRIVATE .
)

target_include_directories(moving-polygon_data_type_plugin
        PUBLIC include
        PRIVATE .
)
