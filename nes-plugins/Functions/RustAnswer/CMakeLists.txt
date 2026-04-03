# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# CXX bridge for Rust FFI
add_subdirectory(rust)

# Register logical function plugin
add_plugin(RustAnswer LogicalFunction nes-logical-operators RustAnswerLogicalFunction.cpp)

# Register physical function plugin — links the CXX bridge for FFI access
add_plugin(RustAnswer PhysicalFunction nes-physical-operators RustAnswerPhysicalFunction.cpp)
target_link_libraries(nes-physical-operators PRIVATE nes-rust-answer-bindings)
target_link_rust_lib(nes-physical-operators nes_rust_answer)
