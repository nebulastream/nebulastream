# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

find_package(Arrow REQUIRED)

add_plugin_as_library(ArrowFile Source nes-sources-registry arrow_file_source_plugin ArrowFileSource.cpp)
add_plugin_as_library(ArrowFile SourceValidation nes-sources-registry arrow_file_source_validation_plugin ArrowFileSource.cpp)

target_include_directories(arrow_file_source_plugin PRIVATE .)
target_include_directories(arrow_file_source_validation_plugin PRIVATE .)

target_link_libraries(arrow_file_source_plugin PRIVATE Arrow::arrow_static)
target_link_libraries(arrow_file_source_validation_plugin PRIVATE Arrow::arrow_static)

if (NES_ENABLES_TESTS)
    add_subdirectory(tests)
endif()
