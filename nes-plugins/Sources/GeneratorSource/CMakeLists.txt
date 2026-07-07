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

add_library(generator_source_plugin_library STATIC
        GeneratorSource.cpp Generator.cpp GeneratorFields.cpp)
target_link_libraries(generator_source_plugin_library PRIVATE nes-sources)
target_include_directories(generator_source_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
target_compile_definitions(generator_source_plugin_library PRIVATE SYSTEST_DATA_DIR="${CMAKE_SOURCE_DIR}/nes-systests/testdata/")
link_plugin_library(nes-sources generator_source_plugin_library)

add_registry_entry(Source Generator)
add_registry_entry(SourceConfigSchema Generator)
add_registry_entry(SourceConfig Generator)
