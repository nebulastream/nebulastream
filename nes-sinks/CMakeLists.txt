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

# Declare the library up front so create_runtime_registry can attach the glue sub-libraries;
# sources are added below once src/ has registered them.
add_library(nes-sinks STATIC)
set_target_properties(nes-sinks PROPERTIES LINKER_LANGUAGE CXX)

# Todo: #72 we can potentially remove nes-data-types, if we move parsing/formatting out of nes-sinks
target_link_libraries(nes-sinks PUBLIC nes-common nes-configurations nes-data-types nes-memory nes-executable nes-network-bindings)

target_include_directories(nes-sinks PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# Runtime registries: a sink plugin registers itself with add_registry_entry(<Registry> <name>).
# Entries are expressed through the plugin's TYPE (included via HEADER_TEMPLATE) and the factory
# templates from the registry headers.
create_runtime_registry(Sink nes-sinks
        ENTRY_TEMPLATE "makeSinkFactory<\${PLUGIN_NAME}Sink>()"
        HEADER_TEMPLATE "\${PLUGIN_NAME}Sink.hpp")

create_runtime_registry(SinkValidation nes-sinks
        ENTRY_TEMPLATE "makeSinkValidator<\${PLUGIN_NAME}Sink>()"
        HEADER_TEMPLATE "\${PLUGIN_NAME}Sink.hpp")

add_subdirectory(src)
get_source(nes-sinks NES_SINKS_SOURCE_FILES)
target_sources(nes-sinks PRIVATE ${NES_SINKS_SOURCE_FILES})

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-sinks REUSE_FROM nes-common)
endif ()
