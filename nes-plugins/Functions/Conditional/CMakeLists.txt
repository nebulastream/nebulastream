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

# Physical function: contributes to the factory registry owned by nes-physical-operators.
add_library(conditional_physical_function_plugin_library STATIC ConditionalPhysicalFunction.cpp)
target_link_libraries(conditional_physical_function_plugin_library PRIVATE nes-physical-operators)
target_include_directories(conditional_physical_function_plugin_library PUBLIC include PRIVATE .)
link_plugin_library(nes-physical-operators conditional_physical_function_plugin_library)
add_registry_entry(PhysicalFunction Conditional KEY Case)

# Each generated glue translation unit includes the plugin header by basename, and the headers of
# this plugin sit one directory deeper than the plugin root that the glue picks up by itself.
foreach (registry IN ITEMS PhysicalFunction)
    get_property(glue_lib GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_GLUE_LIB")
    target_include_directories(${glue_lib} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include/Functions)
endforeach ()
