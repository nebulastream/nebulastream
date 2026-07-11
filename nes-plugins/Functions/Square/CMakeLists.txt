# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generated from: square.extension.md

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)
include(${PROJECT_SOURCE_DIR}/cmake/UnreflectionRegistrationUtil.cmake)

add_plugin_as_library(Square LogicalFunction square_logical_lib SquareLogicalFunction.cpp)
target_include_directories(square_logical_lib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_plugin_as_library(Square PhysicalFunction square_physical_lib SquarePhysicalFunction.cpp)
target_include_directories(square_physical_lib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# add_unreflection_plugin computes the include path relative to a src/ ancestor, which does
# not exist under nes-plugins/. We expose the plugin directory to the glue library so that
# the generated unreflector TU can #include <SquareLogicalFunction.hpp> successfully.
get_property(_unr_lib GLOBAL PROPERTY "LogicalFunction_UNREFLECTION_GLUE_LIB")
target_include_directories(${_unr_lib} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
add_unreflection_plugin(LogicalFunction Square)
