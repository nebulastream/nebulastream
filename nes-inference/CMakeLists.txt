# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

find_package(Boost REQUIRED COMPONENTS graph filesystem process)
find_package(OpenSSL REQUIRED)

add_library(nes-inference src/Model.cpp src/ModelLoader.cpp)
target_include_directories(nes-inference PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include/nebulastream/>
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(nes-inference PUBLIC nes-data-types PRIVATE nes-common Boost::graph Boost::filesystem Boost::process OpenSSL::Crypto)

add_tests_if_enabled(tests)
