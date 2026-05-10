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

# Two registrations from the same source: each declares its own STRUCT layout
# and is reachable via `DataTypeProvider::provideDataType(<name>)`.
add_plugin_as_library(Image DataType nes-data-types-registry image_data_type_plugin ImageDataType.cpp)
add_plugin_as_library(ThermalFrame DataType nes-data-types-registry thermal_frame_data_type_plugin ImageDataType.cpp)
add_plugin_as_library(ThermalPixel DataType nes-data-types-registry thermal_pixel_data_type_plugin ImageDataType.cpp)
add_plugin_as_library(ThermalImage DataType nes-data-types-registry thermal_image_data_type_plugin ImageDataType.cpp)
add_plugin_as_library(RGBFrame DataType nes-data-types-registry rgb_frame_data_type_plugin ImageDataType.cpp)

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
