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

add_plugin(IsNaN LogicalFunction IsNaNLogicalFunction.cpp)
add_plugin(IsNaN PhysicalFunction IsNaNPhysicalFunction.cpp)
add_unreflection_plugin(LogicalFunction IsNaN)

# add_plugin() compiles these sources *into* the component targets, and the generated
# unreflection glue angle-includes the plugin's own header. Neither the components nor the
# glue sub-library know about this directory, so put it on all three include paths.
# In-tree functions get this for free by living under the component's own src/.
target_include_directories(nes-logical-operators PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(nes-physical-operators PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(nes-logical-operators-LogicalFunction-unreflection PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
