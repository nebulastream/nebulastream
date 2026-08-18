# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)
include(${PROJECT_SOURCE_DIR}/cmake/UnreflectionRegistrationUtil.cmake)

add_library(value_or_null_logical_function_plugin_library STATIC ValueOrNullLogicalFunction.cpp)
target_link_libraries(value_or_null_logical_function_plugin_library PRIVATE nes-logical-operators)
target_include_directories(value_or_null_logical_function_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
link_plugin_library(nes-logical-operators value_or_null_logical_function_plugin_library)

add_library(value_or_null_physical_function_plugin_library STATIC ValueOrNullPhysicalFunction.cpp)
target_link_libraries(value_or_null_physical_function_plugin_library PRIVATE nes-physical-operators)
target_include_directories(value_or_null_physical_function_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
link_plugin_library(nes-physical-operators value_or_null_physical_function_plugin_library)

# KEY: the wire/SQL name is value_or_null, but the C++ type is ValueOrNull*Function.
add_registry_entry(LogicalFunction ValueOrNull KEY value_or_null)
add_registry_entry(PhysicalFunction ValueOrNull KEY value_or_null)
add_unreflection_entry(LogicalFunction ValueOrNull KEY value_or_null)
