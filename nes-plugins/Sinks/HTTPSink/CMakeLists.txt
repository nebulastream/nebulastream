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

# One implementation library serving both registries this plugin participates in.
# It used to be two, each compiling HTTPSink.cpp and each pulling in libcurl.
add_library(http_sink_plugin_library STATIC HTTPSink.cpp)
target_link_libraries(http_sink_plugin_library PRIVATE nes-sinks)
target_include_directories(http_sink_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

find_package(CURL REQUIRED)
target_link_libraries(http_sink_plugin_library PRIVATE CURL::libcurl)

link_plugin_library(nes-sinks http_sink_plugin_library)
add_registry_entry(Sink HTTP)
add_registry_entry(SinkValidation HTTP)
