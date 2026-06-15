# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

corrosion_add_cxxbridge(nes-sink-runtime-bindings CRATE nes_sink_runtime_bindings FILES lib.rs)
cxxbridge_remove_rust_link(nes-sink-runtime-bindings nes_sink_runtime_bindings)
target_include_directories(nes-sink-runtime-bindings
        PUBLIC include
)
target_sources(nes-sink-runtime-bindings
    PRIVATE src/SinkRuntimeBindings.cpp
)
target_link_libraries(nes-sink-runtime-bindings PUBLIC nes-buffer-bindings)
