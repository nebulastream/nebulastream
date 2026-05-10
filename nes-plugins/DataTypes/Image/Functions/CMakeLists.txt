# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Image-domain functions live in their own subdirectory rather than alongside
# the data-type plugins above — multiple `add_plugin_as_library` targets in
# the same CMake directory, when split between the DataType and
# (Logical|Physical)Function registries, end up with cross-registry transitive
# `target_link_libraries` chains that Ninja sees as a build-order cycle.
# Splitting by registry into separate subdirs eliminates the same-directory
# inter-target ordering that triggers the cycle.

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

# Logical functions over ThermalFrame. Each gets its own static plugin library so
# the LogicalFunctionRegistry component pulls in the registrar symbols.
# Names follow the SCREAMING_SNAKE_CASE convention used by other plugin functions
# (e.g. TO_BASE64) so SQL `to_celsius(...)` and `is_fever(...)` resolve through
# the parser's case-insensitive lookup against the registered name.
add_plugin_as_library(TO_CELSIUS LogicalFunction nes-logical-operators-registry to_celsius_logical_function ToCelsiusLogicalFunction.cpp)
add_plugin_as_library(IS_FEVER   LogicalFunction nes-logical-operators-registry is_fever_logical_function   IsFeverLogicalFunction.cpp)
add_plugin_as_library(TO_RGB     LogicalFunction nes-logical-operators-registry to_rgb_logical_function     ToRGBLogicalFunction.cpp)
target_include_directories(to_celsius_logical_function PRIVATE .)
target_include_directories(is_fever_logical_function   PRIVATE .)
target_include_directories(to_rgb_logical_function     PRIVATE .)

# Physical (Nautilus) implementations of the same functions. The PhysicalFunctionRegistry
# is keyed on the same NAME the logical function uses, so `FunctionProvider::lowerFunction`
# connects them automatically.
add_plugin_as_library(TO_CELSIUS PhysicalFunction nes-physical-operators-registry to_celsius_physical_function ToCelsiusPhysicalFunction.cpp)
add_plugin_as_library(IS_FEVER   PhysicalFunction nes-physical-operators-registry is_fever_physical_function   IsFeverPhysicalFunction.cpp)
add_plugin_as_library(TO_RGB     PhysicalFunction nes-physical-operators-registry to_rgb_physical_function     ToRGBPhysicalFunction.cpp)
target_include_directories(to_celsius_physical_function PRIVATE . ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_include_directories(is_fever_physical_function   PRIVATE . ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_include_directories(to_rgb_physical_function     PRIVATE . ${CMAKE_CURRENT_SOURCE_DIR}/..)
