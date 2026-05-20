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

add_library(json_value_deserializer_plugin STATIC JSONValueDeserializers.cpp)
target_link_libraries(json_value_deserializer_plugin PRIVATE nes-input-formatters)
target_include_directories(json_value_deserializer_plugin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

link_plugin_library(nes-input-formatters json_value_deserializer_plugin)
add_registry_entry(ValueDeserializer JSONCHAR HEADER JSONValueDeserializers.hpp)
add_registry_entry(ValueDeserializer JSONVARSIZED HEADER JSONValueDeserializers.hpp)
add_registry_entry(ValueDeserializer NullableJSONCHAR HEADER JSONValueDeserializers.hpp)
add_registry_entry(ValueDeserializer NullableJSONVARSIZED HEADER JSONValueDeserializers.hpp)

find_package(simdjson CONFIG REQUIRED)
target_link_libraries(json_value_deserializer_plugin PRIVATE simdjson::simdjson)
