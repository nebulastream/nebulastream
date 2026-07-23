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
# glue sub-library; sources are added below once src/ has registered them.
add_library(nes-sources STATIC)
set_target_properties(nes-sources PROPERTIES LINKER_LANGUAGE CXX)
# PUBLIC nes-input-formatter-provider: SourceDescriptor's public API exposes InputFormatterDescriptor by value.
target_link_libraries(nes-sources PUBLIC nes-common nes-configurations nes-memory nes-executable nes-network-bindings nes-input-formatter-provider)

target_include_directories(nes-sources PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

# Runtime registries: a source plugin registers itself with add_registry_entry(<Registry> <name>).
# Entries are expressed through the plugin's TYPE (included via HEADER_TEMPLATE): construction and
# validation instantiate the factory templates from the registry headers; the systest adaptors are
# static members of the source class.
create_runtime_registry(Source nes-sources
        ENTRY_TEMPLATE "makeSourceFactory<\${PLUGIN_NAME}Source>()"
        HEADER_TEMPLATE "\${PLUGIN_NAME}Source.hpp"
        INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/private)

create_runtime_registry(SourceValidation nes-sources
        ENTRY_TEMPLATE "makeSourceValidator<\${PLUGIN_NAME}Source>()"
        HEADER_TEMPLATE "\${PLUGIN_NAME}Source.hpp"
        INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/private)

# Systest adaptor registries
create_runtime_registry(InlineData nes-sources
        ENTRY_TEMPLATE "&\${PLUGIN_NAME}Source::provideInlineData"
        HEADER_TEMPLATE "\${PLUGIN_NAME}Source.hpp"
        INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/private)

create_runtime_registry(FileData nes-sources
        ENTRY_TEMPLATE "&\${PLUGIN_NAME}Source::provideFileData"
        HEADER_TEMPLATE "\${PLUGIN_NAME}Source.hpp"
        INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/private)

add_subdirectory(src)
get_source(nes-sources NES_SOURCES_SOURCE_FILES)
target_sources(nes-sources PRIVATE ${NES_SOURCES_SOURCE_FILES})

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-sources REUSE_FROM nes-common)
endif ()

add_tests_if_enabled(tests)
