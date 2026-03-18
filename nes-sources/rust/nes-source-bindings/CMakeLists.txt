# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

corrosion_add_cxxbridge(nes-source-bindings CRATE nes_source_bindings FILES lib.rs)
cxxbridge_remove_rust_link(nes-source-bindings nes_source_bindings)
cxxbridge_create_header_target(nes-source-bindings)
target_sources(nes-source-bindings PRIVATE src/SourceBindings.cpp)
target_include_directories(nes-source-bindings
        PRIVATE .
        PUBLIC include
)
# PUBLIC for headers included by SourceBindings.hpp (BufferBindings.hpp, SourceReturnType.hpp)
target_link_libraries(nes-source-bindings PUBLIC nes-buffer-bindings-headers PRIVATE nes-sources)
