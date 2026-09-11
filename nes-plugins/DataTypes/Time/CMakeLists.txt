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

add_subdirectory(Deserializers)
add_subdirectory(Serializers)

add_library(time_datatype_plugin STATIC DateDataType.cpp TimeDataType.cpp TimestampDataType.cpp UnsignedTimestampDataType.cpp)
target_link_libraries(time_datatype_plugin PRIVATE nes-data-types)
target_include_directories(time_datatype_plugin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

link_plugin_library(nes-data-types time_datatype_plugin)
add_registry_entry(DataType Date)
add_registry_entry(DataType Time)
add_registry_entry(DataType Timestamp)
add_registry_entry(DataType UnsignedTimestamp)
