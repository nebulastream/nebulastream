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

# Reading STRUCT registration. Reachable via `DataTypeProvider::provideDataType("Reading")`.
add_plugin_as_library(Reading DataType nes-data-types-registry reading_data_type_plugin ReadingDataType.cpp)

# Per-struct operator overrides for `Reading`. Picked up automatically by
# `FunctionProvider`'s struct-aware dispatch when both operands of the SQL `>` /
# `+` are nominal-typed `Reading`. No core change is needed to add another
# operator/struct combo — a new `add_plugin_as_library(<Op>_Reading_<Other> ...)`
# entry suffices.
add_plugin_as_library(Greater_Reading_Reading PhysicalFunction nes-physical-operators-registry reading_greater_physical_function ReadingGreaterPhysicalFunction.cpp)
add_plugin_as_library(Add_Reading_Reading     PhysicalFunction nes-physical-operators-registry reading_add_physical_function     ReadingAddPhysicalFunction.cpp)
target_include_directories(reading_greater_physical_function PRIVATE .)
target_include_directories(reading_add_physical_function     PRIVATE .)
