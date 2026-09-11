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
add_library(time_deserializers STATIC DefaultTimeDeserializers.cpp UnixEpochTimeDeserializers.cpp)
target_link_libraries(time_deserializers PRIVATE nes-input-formatters)
target_include_directories(time_deserializers PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

link_plugin_library(nes-input-formatters time_deserializers)
add_registry_entry(ValueDeserializer DefaultDate HEADER DefaultTimeDeserializers.hpp)
add_registry_entry(ValueDeserializer DefaultTime HEADER DefaultTimeDeserializers.hpp)
add_registry_entry(ValueDeserializer DefaultTimestamp HEADER DefaultTimeDeserializers.hpp)
add_registry_entry(ValueDeserializer DefaultUnsignedTimestamp HEADER DefaultTimeDeserializers.hpp)
add_registry_entry(ValueDeserializer UnixEpochTimestamp HEADER UnixEpochTimeDeserializers.hpp)
