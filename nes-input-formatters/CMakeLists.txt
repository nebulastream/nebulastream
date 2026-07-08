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

# Declare the library up front so create_runtime_registry can attach the WHOLE_ARCHIVE
# INTERFACE links. Sources are populated below via add_source_files / target_sources.
add_library(nes-input-formatters STATIC)
set_target_properties(nes-input-formatters PROPERTIES LINKER_LANGUAGE CXX)

# Exposing TupleBuffer (nes-memory), PipelineExecutionContext (nes-executable) and the config
# system (nes-configurations, used by the registry headers) from public APIs
target_link_libraries(nes-input-formatters PUBLIC nes-memory nes-executable nes-configurations PRIVATE nes-common nes-runtime nes-executable nes-physical-operators)

# 'Private' nes-input-formatters library that exposes internal headers
target_include_directories(nes-input-formatters PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/InputFormatters>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/InputFormatterProvider>)

# Runtime registries: an input formatter plugin registers itself with add_registry_entry(<registry> <name>).
# By convention the plugin provides a class ${name}InputFormatIndexer with a static getConfigSchema()
# and a static create(const ${name}InputFormatterConfig&, const TupleBufferRef&), and a config struct
# ${name}InputFormatterConfig with a static fromConfig(const InstantiatedConfig&).
create_runtime_registry(InputFormatIndexer nes-input-formatters
        ENTRY_TEMPLATE "makeInputFormatterFactory<\${PLUGIN_NAME}InputFormatIndexer, \${PLUGIN_NAME}InputFormatterConfig>()"
        HEADER_TEMPLATE "\${PLUGIN_NAME}InputFormatIndexer.hpp"
        INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/private ${CMAKE_CURRENT_SOURCE_DIR}/include/InputFormatterProvider)
create_runtime_registry(InputFormatterConfigSchema nes-input-formatters
        ENTRY_TEMPLATE "[] { return \${PLUGIN_NAME}InputFormatIndexer::getConfigSchema(); }"
        HEADER_TEMPLATE "\${PLUGIN_NAME}InputFormatIndexer.hpp"
        INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/private ${CMAKE_CURRENT_SOURCE_DIR}/include/InputFormatterProvider)
create_runtime_registry(InputFormatterConfig nes-input-formatters
        ENTRY_TEMPLATE "makeInputFormatterConfigEntry<\${PLUGIN_NAME}InputFormatterConfig>()"
        HEADER_TEMPLATE "\${PLUGIN_NAME}InputFormatIndexer.hpp"
        INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/private ${CMAKE_CURRENT_SOURCE_DIR}/include/InputFormatterProvider)

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-input-formatters NES_SOURCE_PARSERS_SOURCE_FILES)
target_sources(nes-input-formatters PRIVATE ${NES_SOURCE_PARSERS_SOURCE_FILES})

# 'Public' provider library, only exposing the provider and the TupleBufferRef
add_library(nes-input-formatter-provider src/InputFormatterProvider.cpp)
target_link_libraries(nes-input-formatter-provider PUBLIC nes-memory nes-executable PRIVATE nes-input-formatters)
target_include_directories(nes-input-formatter-provider PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/InputFormatterProvider>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/registry/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-input-formatters REUSE_FROM nes-common)
endif ()

add_tests_if_enabled(tests)
