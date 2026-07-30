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

add_plugin(FMT LogicalFunction nes-logical-operators FmtLogicalFunction.cpp)
add_plugin(to_string LogicalFunction nes-logical-operators ToStringLogicalFunction.cpp)

add_plugin(FMT PhysicalFunction nes-physical-operators FmtPhysicalFunction.cpp)
add_plugin(to_string PhysicalFunction nes-physical-operators ToStringPhysicalFunction.cpp)
# Float / int formatting goes through std::to_chars from <charconv> — no extra link deps.
