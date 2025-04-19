# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-logical-operators NES_LOGICAL_OPERATORS_SOURCE_FILES)

# Add Library
add_library(nes-logical-operators ${NES_LOGICAL_OPERATORS_SOURCE_FILES}
        registry/include/WindowAggregationFunctionRegistry.hpp
        registry/templates/WindowAggregationFunctionGeneratedRegistrar.inc.in)
target_link_libraries(nes-logical-operators PUBLIC nes-sources nes-sinks)
target_include_directories(nes-logical-operators PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# Create library for the registries
create_plugin_registry_library(nes-logical-operators-registry nes-logical-operators)
target_link_libraries(nes-logical-operators PRIVATE nes-logical-operators-registry)
# Generate a registrar for every registry
generate_plugin_registrars(nes-logical-operators LogicalOperator LogicalFunction WindowAggregationFunction)
