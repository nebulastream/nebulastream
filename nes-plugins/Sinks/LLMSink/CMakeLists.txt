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

add_library(llm_sink_plugin_library STATIC LLMSink.cpp)
target_link_libraries(llm_sink_plugin_library PRIVATE nes-sinks)
target_include_directories(llm_sink_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

find_package(CURL REQUIRED)
target_link_libraries(llm_sink_plugin_library PRIVATE CURL::libcurl)

link_plugin_library(nes-sinks llm_sink_plugin_library)
add_registry_entry(Sink LLM)
add_registry_entry(SinkValidation LLM)
