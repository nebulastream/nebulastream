# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

find_package(magic_enum CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

corrosion_add_cxxbridge(
        nes-logger-bindings
        CRATE nes_rust_bindings
        FILES ../spdlog/lib.rs
)
target_sources(nes-logger-bindings PRIVATE src/LoggerBindings.cpp)
target_link_libraries(nes-logger-bindings PUBLIC nes-common spdlog::spdlog magic_enum::magic_enum)
target_include_directories(nes-logger-bindings PUBLIC include)
