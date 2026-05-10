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

add_library(reading_datatype_plugin STATIC ReadingDataType.cpp)
target_link_libraries(reading_datatype_plugin PRIVATE nes-data-types)
target_include_directories(reading_datatype_plugin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# Reading STRUCT registration. Reachable via `DataTypeProvider::provideDataType("Reading")`.
link_plugin_library(nes-data-types reading_datatype_plugin)
add_registry_entry(DataType Reading)

add_library(reading_physical_functions STATIC ReadingGreaterPhysicalFunction.cpp ReadingAddPhysicalFunction.cpp)
target_link_libraries(reading_physical_functions PRIVATE nes-physical-operators)
target_include_directories(reading_physical_functions PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# Per-struct operator overrides for `Reading`. Picked up automatically by
# `FunctionProvider`'s struct-aware dispatch when both operands of the SQL `>` /
# `+` are nominal-typed `Reading`. No core change is needed to add another
# operator/struct combo — a new `add_plugin_as_library(<Op>_Reading_<Other> ...)`
# entry suffices.
link_plugin_library(nes-physical-operators reading_physical_functions)
add_registry_entry(PhysicalFunction ReadingGreater KEY Greater_Reading_Reading)
add_registry_entry(PhysicalFunction ReadingAdd KEY Add_Reading_Reading)
