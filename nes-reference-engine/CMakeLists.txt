# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#    https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if(NOT NES_USE_REFERENCE_QUERY_ENGINE)
    return()
endif()

if(NOT EXISTS "${CMAKE_SOURCE_DIR}/../reference-engine/query-manager/Cargo.toml")
    message(FATAL_ERROR "NES_USE_REFERENCE_QUERY_ENGINE requires ../reference-engine/query-manager")
endif()

corrosion_import_crate(
        MANIFEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/rust/reference-adapter/Cargo.toml
        CRATES nes_reference_adapter
        CRATE_TYPES staticlib
        FLAGS ${NES_RUST_CARGO_FLAGS}
)
set_property(TARGET nes_reference_adapter PROPERTY CORROSION_ENVIRONMENT_VARIABLES ${NES_RUST_ENV_VARS})
register_rust_crate(nes_reference_adapter "${CMAKE_CURRENT_SOURCE_DIR}/rust/reference-adapter")

add_library(nes-reference-engine ReferenceQueryEngine.cpp ReferenceNodeEngine.cpp)
target_include_directories(nes-reference-engine PUBLIC include)
target_link_libraries(nes-reference-engine PUBLIC nes-runtime nes-executable nes-sources nes-query-engine-interface)
target_link_rust_lib(nes-reference-engine nes_reference_adapter)
add_dependencies(nes-reference-engine cargo-build_nes_reference_adapter)
