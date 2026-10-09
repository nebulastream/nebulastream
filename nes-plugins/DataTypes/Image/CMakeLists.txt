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

add_library(image_datatype_plugin STATIC ImageDataType.cpp)
target_link_libraries(image_datatype_plugin PRIVATE nes-data-types)
target_include_directories(image_datatype_plugin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Multiple from the same source: each declares its own STRUCT layout
# and is reachable via `DataTypeProvider::provideDataType(<name>)`.
link_plugin_library(nes-data-types image_datatype_plugin)
add_registry_entry(DataType Image)
add_registry_entry(DataType ThermalFrame HEADER ImageDataType.hpp)
add_registry_entry(DataType ThermalPixel HEADER ImageDataType.hpp)
add_registry_entry(DataType ThermalImage HEADER ImageDataType.hpp)
add_registry_entry(DataType RGBFrame HEADER ImageDataType.hpp)

# The Image-domain function plugins (to_celsius, is_fever, to_rgb) live in a
# sibling subdirectory to keep the DataType-registry plugins above isolated
# from cross-registry build-order cycles that arise when multiple plugin
# libraries from different registries co-exist in the same CMakeLists dir.
add_subdirectory(Functions)

# Domain-specific concretization of StructData for ThermalFrame. Lives outside
# the registry because it isn't a DataType/LogicalFunction itself — it's a
# typed view used by physical/runtime code (and our unit tests).
add_library(image_thermal_frame_lib STATIC ThermalFrameData.cpp)
target_link_libraries(image_thermal_frame_lib PUBLIC nes-nautilus)
target_include_directories(image_thermal_frame_lib PUBLIC .)

if (NES_ENABLES_TESTS)
    add_subdirectory(tests)
endif ()
