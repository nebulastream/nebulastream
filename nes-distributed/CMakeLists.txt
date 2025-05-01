# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

activate_optional_plugin("Sources/NetworkSource" ON)
activate_optional_plugin("Sinks/NetworkSink" ON)

add_library(nes-distributed network/Bridge.cpp)
target_include_directories(nes-distributed
        PRIVATE .
        PUBLIC include
)

corrosion_add_cxxbridge(
        cxx_bridge
        CRATE bridge_of_bridges
        FILES ../network/lib.rs
)
target_include_directories(cxx_bridge PUBLIC network/include)
target_link_libraries(cxx_bridge PUBLIC nes-memory)

target_link_libraries(nes-distributed PUBLIC nes-common cxx_bridge)
