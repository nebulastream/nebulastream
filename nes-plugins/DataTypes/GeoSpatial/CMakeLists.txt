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
add_plugin_as_library(Point DataType nes-data-types-registry point_data_type_plugin PointDataType.cpp)
add_plugin_as_library(Line DataType nes-data-types-registry line_data_type_plugin LineDataType.cpp)
add_plugin_as_library(Circle DataType nes-data-types-registry circle_data_type_plugin CircleDataType.cpp)

target_include_directories(point_data_type_plugin
        PUBLIC include
        PRIVATE .
)

target_include_directories(line_data_type_plugin
        PUBLIC include
        PRIVATE .
)

target_include_directories(circle_data_type_plugin
        PUBLIC include
        PRIVATE .
)
