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

add_library(llmr_source_plugin_library STATIC LLMRSource.cpp LLMRDataServer.cpp)
target_link_libraries(llmr_source_plugin_library PRIVATE nes-sources)
target_include_directories(llmr_source_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

link_plugin_library(nes-sources llmr_source_plugin_library)
add_registry_entry(Source LLMR)
add_registry_entry(SourceValidation LLMR)
add_registry_entry(InlineData LLMR)
add_registry_entry(FileData LLMR)
