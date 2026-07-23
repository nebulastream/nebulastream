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

# One implementation library serving all registries this plugin participates in.
add_library(checksum_sink_plugin STATIC ChecksumSink.cpp)
target_link_libraries(checksum_sink_plugin PRIVATE nes-sinks)
# PUBLIC: ChecksumSink.hpp includes Checksum.hpp, so the registration glue (which includes the
# plugin header) needs systest-checksum's include directories propagated.
target_link_libraries(checksum_sink_plugin PUBLIC systest-checksum)
target_include_directories(checksum_sink_plugin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

link_plugin_library(nes-sinks checksum_sink_plugin)
add_registry_entry(Sink Checksum)
add_registry_entry(SinkValidation Checksum)
