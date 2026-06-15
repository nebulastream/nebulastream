# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

corrosion_add_cxxbridge(nes-io-runtime-bindings CRATE nes_io_runtime_bindings FILES lib.rs)
cxxbridge_remove_rust_link(nes-io-runtime-bindings nes_io_runtime_bindings)
target_include_directories(nes-io-runtime-bindings
        PUBLIC include
)
target_sources(nes-io-runtime-bindings
    PRIVATE src/IORuntimeBindings.cpp
)
target_link_libraries(nes-io-runtime-bindings PRIVATE nes-common)
