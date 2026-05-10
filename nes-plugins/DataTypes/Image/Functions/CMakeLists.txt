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

# Registry keys follow the SCREAMING_SNAKE_CASE convention used by other plugin functions
# (e.g. TO_BASE64) so SQL `to_celsius(...)` and `is_fever(...)` resolve through the parser's
# case-insensitive lookup against the registered name. The PhysicalFunctionRegistry is keyed on
# the same name, so `FunctionProvider::lowerFunction` connects logical and physical functions.

# Logical functions over ThermalFrame: contribute to the factory and the serialized plan
# reconstruction registries owned by nes-logical-operators.
add_library(image_logical_functions_plugin_library STATIC
        ToCelsiusLogicalFunction.cpp
        IsFeverLogicalFunction.cpp
        ToRGBLogicalFunction.cpp)
target_link_libraries(image_logical_functions_plugin_library PRIVATE nes-logical-operators)
target_include_directories(image_logical_functions_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
link_plugin_library(nes-logical-operators image_logical_functions_plugin_library)
add_registry_entry(LogicalFunction ToCelsius KEY TO_CELSIUS)
add_registry_entry(LogicalFunction IsFever KEY IS_FEVER)
add_registry_entry(LogicalFunction ToRGB KEY TO_RGB)
add_unreflection_entry(LogicalFunction ToCelsius KEY TO_CELSIUS)
add_unreflection_entry(LogicalFunction IsFever KEY IS_FEVER)
add_unreflection_entry(LogicalFunction ToRGB KEY TO_RGB)

# Physical (Nautilus) implementations: contribute to the factory registry owned by nes-physical-operators.
add_library(image_physical_functions_plugin_library STATIC
        ToCelsiusPhysicalFunction.cpp
        IsFeverPhysicalFunction.cpp
        ToRGBPhysicalFunction.cpp)
target_link_libraries(image_physical_functions_plugin_library PRIVATE nes-physical-operators)
target_include_directories(image_physical_functions_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
link_plugin_library(nes-physical-operators image_physical_functions_plugin_library)
add_registry_entry(PhysicalFunction ToCelsius KEY TO_CELSIUS)
add_registry_entry(PhysicalFunction IsFever KEY IS_FEVER)
add_registry_entry(PhysicalFunction ToRGB KEY TO_RGB)
