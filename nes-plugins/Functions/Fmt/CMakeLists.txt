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

add_plugin(FMT LogicalFunction FmtLogicalFunction.cpp)
add_plugin(to_string LogicalFunction ToStringLogicalFunction.cpp)

add_plugin(FMT PhysicalFunction FmtPhysicalFunction.cpp)
add_plugin(to_string PhysicalFunction ToStringPhysicalFunction.cpp)
# Float / int formatting goes through std::to_chars from <charconv> — no extra link deps.

# KEY: the wire/SQL names are FMT and to_string; the C++ types are Fmt/ToStringLogicalFunction.
add_unreflection_plugin(LogicalFunction Fmt KEY FMT)
add_unreflection_plugin(LogicalFunction ToString KEY to_string)

# See IsNaN/CMakeLists.txt for why these three include paths are needed.
target_include_directories(nes-logical-operators PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(nes-physical-operators PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(nes-logical-operators-LogicalFunction-unreflection PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
