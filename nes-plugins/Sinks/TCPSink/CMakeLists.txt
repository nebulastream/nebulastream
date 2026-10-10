# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)

# Boost.Scope is header-only.
find_package(Boost CONFIG REQUIRED)
find_package(c-ares CONFIG REQUIRED)

add_library(tcp_sink_plugin_library STATIC TCPSink.cpp TCPChannel.cpp)
target_link_libraries(tcp_sink_plugin_library PRIVATE nes-sinks c-ares::cares Boost::headers)
target_include_directories(tcp_sink_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

link_plugin_library(nes-sinks tcp_sink_plugin_library)
add_registry_entry(Sink TCP)
add_registry_entry(SinkValidation TCP)

add_e2e_test(
    NAME tcp-sink-test
    BATS_FILE tests/distributed.bats
    DOCKER_COMPOSE
)
