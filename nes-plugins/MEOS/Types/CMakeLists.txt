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
add_library(meos_data_types STATIC TemporalPointDataType.cpp TemporalPointSequenceDataType.cpp SpatioTemporalBoxDataType.cpp PolygonDataType.cpp TemporalPolygonDataType.cpp)
target_link_libraries(meos_data_types PRIVATE nes-data-types)
target_include_directories(meos_data_types PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(Functions)

link_plugin_library(nes-data-types meos_data_types)
add_registry_entry(DataType TemporalPoint)
add_registry_entry(DataType TemporalPointSequence)
add_registry_entry(DataType SpatioTemporalBox)
add_registry_entry(DataType Polygon)
add_registry_entry(DataType TemporalPolygon)
