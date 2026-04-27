# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Mirrors nes-sources: two targets built from one directory. See that CMakeLists
# for the rationale.
#   nes-sinks-validation — SinkDescriptor / SinkCatalog / Register*SinkValidation
#                          TUs. Minimal deps; safe for nes-frontend.
#   nes-sinks            — runtime sinks (Sink, SinkProvider, TokioSink, per-sink
#                          execution). Depends on validation.
add_subdirectory(src)
get_source(nes-sinks-validation NES_SINKS_VALIDATION_SOURCE_FILES)
get_source(nes-sinks NES_SINKS_SOURCE_FILES)

add_library(nes-sinks-validation ${NES_SINKS_VALIDATION_SOURCE_FILES})
add_library(nes-sinks ${NES_SINKS_SOURCE_FILES})

add_subdirectory(rust/nes-sink-validation-bindings)
add_subdirectory(rust/nes-sink-runtime-bindings)

target_link_libraries(nes-sinks-validation
        PUBLIC nes-common nes-configurations nes-data-types nes-sink-validation-bindings)
target_link_rust_lib(nes-sinks-validation nes_sink_validation_bindings)

target_include_directories(nes-sinks-validation PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# Todo: #72 we can potentially remove nes-data-types, if we move parsing/formatting out of nes-sinks
target_link_libraries(nes-sinks
        PUBLIC nes-sinks-validation nes-common nes-configurations nes-data-types nes-memory nes-executable
        PRIVATE nes-io-runtime nes-sink-runtime-bindings)
target_link_rust_lib(nes-sinks nes_sink_runtime_bindings)

target_include_directories(nes-sinks PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>
        $<INSTALL_INTERFACE:include/nebulastream/>)


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-sinks-validation REUSE_FROM nes-common)
    target_precompile_headers(nes-sinks REUSE_FROM nes-common)
endif ()

create_registries_for_component(COMPONENT nes-sinks-validation SinkValidation)
create_registries_for_component(COMPONENT nes-sinks Sink)
