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

add_library(fmt_logical_function_plugin_library STATIC FmtLogicalFunction.cpp ToStringLogicalFunction.cpp)
target_link_libraries(fmt_logical_function_plugin_library PRIVATE nes-logical-operators)
target_include_directories(fmt_logical_function_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
link_plugin_library(nes-logical-operators fmt_logical_function_plugin_library)

# Float / int formatting goes through std::to_chars from <charconv> — no extra link deps.
add_library(fmt_physical_function_plugin_library STATIC FmtPhysicalFunction.cpp ToStringPhysicalFunction.cpp)
target_link_libraries(fmt_physical_function_plugin_library PRIVATE nes-physical-operators)
target_include_directories(fmt_physical_function_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
link_plugin_library(nes-physical-operators fmt_physical_function_plugin_library)

# KEY: the wire/SQL names are FMT and to_string; the C++ types are Fmt/ToString*Function.
add_registry_entry(LogicalFunction Fmt KEY FMT)
add_registry_entry(LogicalFunction ToString KEY to_string)
add_registry_entry(PhysicalFunction Fmt KEY FMT)
add_registry_entry(PhysicalFunction ToString KEY to_string)
add_unreflection_entry(LogicalFunction Fmt KEY FMT)
add_unreflection_entry(LogicalFunction ToString KEY to_string)
