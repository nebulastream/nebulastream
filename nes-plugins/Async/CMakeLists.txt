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

find_package(reflectcpp CONFIG REQUIRED)

# Asynchronous execution: the Handoff sink ends the producer half of a split async operator, the Async source
# runs its calls and starts the consumer half. Both meet in the in-process HandoffChannel.
add_library(async_handoff_channel STATIC HandoffChannel.cpp)
target_link_libraries(async_handoff_channel PUBLIC nes-memory)
target_include_directories(async_handoff_channel PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_library(async_source_plugin_library STATIC AsyncSource.cpp)
target_link_libraries(async_source_plugin_library PRIVATE nes-sources nes-nautilus reflectcpp::reflectcpp PUBLIC async_handoff_channel)
link_plugin_library(nes-sources async_source_plugin_library)
add_registry_entry(Source Async)
add_registry_entry(SourceValidation Async)

add_library(handoff_sink_plugin_library STATIC HandoffSink.cpp)
target_link_libraries(handoff_sink_plugin_library PRIVATE nes-sinks PUBLIC async_handoff_channel)
link_plugin_library(nes-sinks handoff_sink_plugin_library)
add_registry_entry(Sink Handoff)
add_registry_entry(SinkValidation Handoff)

# End-to-end tests in the embedded REPL against tests/http_path_server.py.
add_e2e_test(
    NAME async-repl-test
    BATS_FILE tests/async.bats
)
