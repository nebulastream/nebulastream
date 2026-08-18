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

add_library(isnan_logical_function_plugin_library STATIC IsNaNLogicalFunction.cpp)
target_link_libraries(isnan_logical_function_plugin_library PRIVATE nes-logical-operators)
target_include_directories(isnan_logical_function_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
link_plugin_library(nes-logical-operators isnan_logical_function_plugin_library)

add_library(isnan_physical_function_plugin_library STATIC IsNaNPhysicalFunction.cpp)
target_link_libraries(isnan_physical_function_plugin_library PRIVATE nes-physical-operators)
target_include_directories(isnan_physical_function_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
link_plugin_library(nes-physical-operators isnan_physical_function_plugin_library)

add_registry_entry(LogicalFunction IsNaN)
add_registry_entry(PhysicalFunction IsNaN)
add_unreflection_entry(LogicalFunction IsNaN)
