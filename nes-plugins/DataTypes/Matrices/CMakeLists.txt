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

add_library(matrices_data_type_plugins STATIC 3x3MatrixDataType.cpp)
target_link_libraries(matrices_data_type_plugins PRIVATE nes-data-types)
target_include_directories(matrices_data_type_plugins PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

link_plugin_library(nes-data-types matrices_data_type_plugins)
add_registry_entry(DataType 3x3Matrix KEY Matrix)
