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
get_source(nes-sources NES_SOURCES_SOURCE_FILES)

add_library(nes-sources ${NES_SOURCES_SOURCE_FILES})
target_link_libraries(nes-sources PUBLIC nes-common nes-configurations nes-memory nes-executable)

target_include_directories(nes-sources PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-sources REUSE_FROM nes-common)
endif ()

create_registries_for_component(Source SourceValidation TokioSource FileData InlineData)

add_subdirectory(rust/nes-source-bindings)

# nes-sources (TokioSource.cpp) includes CXX-generated headers from nes-io-bindings.
# Use PRIVATE link to get the include directories without creating a PUBLIC circular dependency
# (nes-io-bindings already links PUBLIC to nes-sources for the SourceReturnType headers).
target_link_libraries(nes-sources PRIVATE nes-io-bindings)

# Two Rust static libraries (nes_source_lib via nes-io-bindings, nes_rust_bindings via nes-network)
# both contain identical CXX runtime symbols. This is safe — the symbols are identical object code
# from the same cxx crate version. Allow the linker to deduplicate them.
target_link_options(nes-sources PUBLIC "LINKER:--allow-multiple-definition")

add_tests_if_enabled(tests)
