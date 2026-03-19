# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Corrosion resolves FILES as <manifest_dir>/src/<path>, so ../nes-source-bindings/lib.rs
# becomes nes-source-lib/src/../nes-source-bindings/lib.rs = nes-source-lib/nes-source-bindings/lib.rs.
# The directory component also controls the generated header include path (e.g. <nes-source-lib/src/lib.h>).
# Create symlinks inside nes-source-lib/ so the source paths resolve on a fresh clone.
set(_lib_dir "${CMAKE_CURRENT_SOURCE_DIR}/../nes-source-lib")
if(NOT EXISTS "${_lib_dir}/nes-source-bindings")
    file(CREATE_LINK "${CMAKE_CURRENT_SOURCE_DIR}" "${_lib_dir}/nes-source-bindings" SYMBOLIC)
endif()
if(NOT EXISTS "${_lib_dir}/nes-source-lib")
    file(CREATE_LINK "${_lib_dir}" "${_lib_dir}/nes-source-lib" SYMBOLIC)
endif()

corrosion_add_cxxbridge(
        nes-io-bindings
        CRATE nes_source_lib
        FILES ../nes-source-bindings/lib.rs ../nes-source-lib/src/lib.rs
)
cxxbridge_remove_rust_link(nes-io-bindings nes_source_lib)
target_sources(nes-io-bindings PRIVATE src/IoBindings.cpp)
target_include_directories(nes-io-bindings
        PRIVATE .
        PUBLIC include
)
target_link_libraries(nes-io-bindings PUBLIC nes-memory nes-sources nes-executable)
