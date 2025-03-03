# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-common
        NesLogger.cpp
        LogLevel.cpp
        spdlog/bridge.cpp
)

corrosion_add_cxxbridge(
        rustlogger_cxx_bridge
        CRATE bridge_of_bridges
        FILES ../spdlog/lib.rs
)
target_include_directories(rustlogger_cxx_bridge PUBLIC spdlog/include)
target_link_libraries(rustlogger_cxx_bridge PUBLIC spdlog::spdlog magic_enum::magic_enum)

find_package(magic_enum CONFIG REQUIRED)
find_package(spdlog     CONFIG REQUIRED)

