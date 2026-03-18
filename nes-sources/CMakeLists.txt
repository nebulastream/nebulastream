# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(src)

# --- Validation-only target (no runtime deps) ---
add_library(nes-sources-validation
    src/FileSourceValidation.cpp
    src/SourceValidationProvider.cpp
)
target_link_libraries(nes-sources-validation PUBLIC nes-common nes-configurations nes-data-types)
target_include_directories(nes-sources-validation PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
)

# Create validation registry (uses existing registry/templates/ in nes-sources/)
create_plugin_registry_library(nes-sources-validation-registry nes-sources-validation)
target_link_libraries(nes-sources-validation PRIVATE nes-sources-validation-registry)
generate_plugin_registrars(nes-sources-validation SourceValidation)

# --- Runtime target ---
get_source(nes-sources NES_SOURCES_SOURCE_FILES)
add_library(nes-sources ${NES_SOURCES_SOURCE_FILES})
target_link_libraries(nes-sources PUBLIC nes-sources-validation nes-common nes-configurations nes-memory nes-executable nes-network-bindings)

target_include_directories(nes-sources PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-sources REUSE_FROM nes-common)
endif ()

# Runtime registries (Source, FileData, InlineData -- NOT SourceValidation, owned by validation target)
create_registries_for_component(Source FileData InlineData)

add_tests_if_enabled(tests)
