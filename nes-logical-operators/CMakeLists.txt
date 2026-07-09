# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/UnreflectionRegistrationUtil.cmake)

# Declare the library up front so create_unreflection_registry can attach the
# WHOLE_ARCHIVE INTERFACE link, and so add_subdirectory(src) can add to its
# target_sources. Sources are populated below via add_source_files / target_sources.
add_library(nes-logical-operators STATIC)
set_target_properties(nes-logical-operators PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(nes-logical-operators PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# Unreflection registries — must be declared after the parent target exists and
# before add_subdirectory(src) so subdir add_unreflection_plugin calls find them.
create_unreflection_registry(LogicalFunction nes-logical-operators
        "\${PLUGIN_NAME}LogicalFunction"
        "\${PLUGIN_NAME}LogicalFunction.hpp")
create_unreflection_registry(LogicalOperator nes-logical-operators
        "TypedLogicalOperator<\${PLUGIN_NAME}LogicalOperator>"
        "\${PLUGIN_NAME}LogicalOperator.hpp")
create_unreflection_registry(AggregationLogicalFunction nes-logical-operators
        "\${PLUGIN_NAME}AggregationLogicalFunction"
        "\${PLUGIN_NAME}AggregationLogicalFunction.hpp")

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-logical-operators NES_LOGICAL_OPERATORS_SOURCE_FILES)
target_sources(nes-logical-operators PRIVATE ${NES_LOGICAL_OPERATORS_SOURCE_FILES})

find_package(reflectcpp REQUIRED)

target_link_libraries(nes-logical-operators PUBLIC nes-sources nes-sinks nes-query-optimizer nes-data-types nes-inference nes-udf PRIVATE reflectcpp::reflectcpp)

# Factory registries
create_registries_for_component(LogicalOperator LogicalFunction AggregationLogicalFunction)

add_tests_if_enabled(tests)
