# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin_as_library(Video Source video_source_plugin_library VideoSource.cpp)
add_plugin_as_library(Video SourceValidation video_source_validation_plugin_library VideoSource.cpp)

target_include_directories(video_source_plugin_library PRIVATE .)
target_include_directories(video_source_validation_plugin_library PRIVATE .)

find_package(PkgConfig REQUIRED)
pkg_check_modules(ARAVIS REQUIRED IMPORTED_TARGET aravis-0.8)
target_link_libraries(video_source_plugin_library PRIVATE PkgConfig::ARAVIS)
target_link_libraries(video_source_validation_plugin_library PRIVATE PkgConfig::ARAVIS)

add_executable(video_source_smoke_test VideoSourceSmokeTest.c)
target_link_libraries(video_source_smoke_test PRIVATE PkgConfig::ARAVIS)
