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
add_library(nes-output-formatters STATIC)
set_target_properties(nes-output-formatters PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(nes-output-formatters PUBLIC nes-common nes-data-types nes-memory nes-nautilus)

target_include_directories(nes-output-formatters PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

# Runtime registries: entries are expressed through the plugin's TYPE (included via
# HEADER_TEMPLATE): static provideFormatter members for construction (formatter constructors
# differ), the validator factory template for validation.
create_runtime_registry(OutputFormatter nes-output-formatters
        ENTRY_TEMPLATE "&\${PLUGIN_NAME}OutputFormatter::provideFormatter"
        HEADER_TEMPLATE "\${PLUGIN_NAME}OutputFormatter.hpp"
        INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/private)

create_runtime_registry(OutputFormatterValidation nes-output-formatters
        ENTRY_TEMPLATE "makeOutputFormatterValidator<\${PLUGIN_NAME}OutputFormatter>()"
        HEADER_TEMPLATE "\${PLUGIN_NAME}OutputFormatter.hpp"
        INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/private)

add_subdirectory(src)
get_source(nes-output-formatters NES_OUTPUT_FORMATTERS_SOURCE_FILES)
target_sources(nes-output-formatters PRIVATE ${NES_OUTPUT_FORMATTERS_SOURCE_FILES})

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-output-formatters REUSE_FROM nes-common)
endif ()
