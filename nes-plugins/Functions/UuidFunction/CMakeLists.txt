# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin_as_library(Uuid LogicalFunction nes-logical-operators uuid_logical_function_plugin UuidLogicalFunction.cpp)
target_link_libraries(uuid_logical_function_plugin PRIVATE nes-logical-operators-registry)

add_plugin_as_library(Uuid PhysicalFunction nes-physical-operators uuid_physical_function_plugin UuidPhysicalFunction.cpp)
find_package(unofficial-libuuid CONFIG REQUIRED)
target_link_libraries(uuid_physical_function_plugin PRIVATE nes-physical-operators-registry nes-nautilus unofficial::UUID::uuid)
