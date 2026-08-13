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
add_library(nes-output-formatters STATIC)
set_target_properties(nes-output-formatters PROPERTIES LINKER_LANGUAGE CXX)

# PUBLIC nes-configurations: the registry headers and the descriptor expose the config system.
target_link_libraries(nes-output-formatters PUBLIC nes-common nes-data-types nes-memory nes-nautilus nes-configurations)

target_include_directories(nes-output-formatters PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

# Runtime registries: an output formatter plugin registers itself with add_registry_entry(<registry> <name>).
# By convention the plugin provides a class ${name}OutputFormatter with a static getConfigSchema()
# and a static provideFormatter (formatter constructors differ), and a config struct
# ${name}OutputFormatterConfig with a static fromConfig(const InstantiatedConfig&).
create_runtime_registry(OutputFormatter nes-output-formatters
        ENTRY_TEMPLATE "&\${PLUGIN_NAME}OutputFormatter::provideFormatter"
        HEADER_TEMPLATE "\${PLUGIN_NAME}OutputFormatter.hpp"
        INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/private)

create_runtime_registry(OutputFormatterConfigSchema nes-output-formatters
        ENTRY_TEMPLATE "[] { return \${PLUGIN_NAME}OutputFormatter::getConfigSchema(); }"
        HEADER_TEMPLATE "\${PLUGIN_NAME}OutputFormatter.hpp"
        INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/private)

create_runtime_registry(OutputFormatterConfig nes-output-formatters
        ENTRY_TEMPLATE "makeOutputFormatterConfigEntry<\${PLUGIN_NAME}OutputFormatterConfig>()"
        HEADER_TEMPLATE "\${PLUGIN_NAME}OutputFormatter.hpp"
        INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/private)

add_subdirectory(src)
get_source(nes-output-formatters NES_OUTPUT_FORMATTERS_SOURCE_FILES)
target_sources(nes-output-formatters PRIVATE ${NES_OUTPUT_FORMATTERS_SOURCE_FILES})

# 'Public' provider library: exposes the descriptor, the provider, and the config registries
# without dragging the nautilus-dependent formatter interfaces into consumers (e.g. nes-sinks,
# whose SinkDescriptor carries an OutputFormatterDescriptor).
add_library(nes-output-formatter-provider src/OutputFormatterProvider.cpp
        include/OutputFormatters/CSVOutputFormatterConfig.hpp)
target_link_libraries(nes-output-formatter-provider PUBLIC nes-configurations nes-memory PRIVATE nes-output-formatters)
target_include_directories(nes-output-formatter-provider PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/registry/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)
