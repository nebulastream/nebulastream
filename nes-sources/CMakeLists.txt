# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This component produces two libraries from one directory:
#   nes-sources-validation — descriptor/catalog/validation types. Minimal deps.
#                            Safe to link from validation-only binaries such as
#                            nes-frontend, where pulling the runtime would drag
#                            in tokio/rand/io-runtime/etc.
#   nes-sources            — runtime source machinery (Source, SourceThread,
#                            TokioSource, rate control). Depends on validation.
# Plugin registrations are split so SourceValidation plugins register against
# nes-sources-validation and Source/InlineData/FileData plugins register against
# nes-sources.
add_subdirectory(src)
get_source(nes-sources-validation NES_SOURCES_VALIDATION_SOURCE_FILES)
get_source(nes-sources NES_SOURCES_SOURCE_FILES)

add_library(nes-sources-validation ${NES_SOURCES_VALIDATION_SOURCE_FILES})
add_library(nes-sources ${NES_SOURCES_SOURCE_FILES})

add_subdirectory(rust/nes-source-validation-bindings)
add_subdirectory(rust/nes-source-runtime-bindings)

target_link_libraries(nes-sources-validation
        PUBLIC nes-common nes-configurations nes-data-types nes-source-validation-bindings)

target_include_directories(nes-sources-validation
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        # FileSourceValidation.cpp needs ConfigParametersCSV + FileSource::NAME,
        # which live in private/FileSource.hpp alongside the runtime class.
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

target_link_libraries(nes-sources
        PUBLIC nes-sources-validation nes-common nes-configurations nes-data-types nes-memory nes-executable nes-network-bindings
        PRIVATE nes-input-formatter-provider nes-runtime nes-io-runtime nes-source-runtime-bindings)

target_include_directories(nes-sources PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-sources-validation REUSE_FROM nes-common)
    target_precompile_headers(nes-sources REUSE_FROM nes-common)
endif ()

create_registries_for_component(COMPONENT nes-sources-validation SourceValidation)
create_registries_for_component(COMPONENT nes-sources Source FileData InlineData)


target_link_rust_lib(nes-sources-validation nes_source_validation_bindings)
target_link_rust_lib(nes-sources nes_network_bindings)
target_link_rust_lib(nes-sources nes_source_runtime_bindings)
add_tests_if_enabled(tests)
