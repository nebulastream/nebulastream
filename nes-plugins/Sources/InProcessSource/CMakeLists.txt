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
add_library(inprocess_source_plugin_library STATIC InProcessSource.cpp)
target_link_libraries(inprocess_source_plugin_library PRIVATE nes-sources)
target_include_directories(inprocess_source_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

link_plugin_library(nes-sources inprocess_source_plugin_library)
add_registry_entry(Source InProcess)
add_registry_entry(SourceValidation InProcess)
# No InlineData/FileData entries: the feed is filled by another component of the worker process, so there
# is nothing for a systest to inject.
