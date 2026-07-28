# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin_as_library(SampleTick Source sample_tick_source_plugin_library SampleTickSource.cpp)
add_plugin_as_library(SampleTick SourceValidation sample_tick_validation_plugin_library SampleTickSource.cpp)

target_include_directories(sample_tick_source_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
target_include_directories(sample_tick_validation_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
