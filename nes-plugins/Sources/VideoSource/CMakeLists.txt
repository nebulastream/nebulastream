# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin_as_library(Video Source nes-sources-registry video_source_plugin_library VideoSource.cpp)
add_plugin_as_library(Video SourceValidation nes-sources-registry video_source_validation_plugin_library VideoSource.cpp)

# The Video source requires aravis to be installed on the system
# https://github.com/aravisProject/aravis
# Note: For our demo we use a Fluke Thermal Camera which requires the endianness patch
find_package(PkgConfig)
pkg_check_modules(aravis REQUIRED IMPORTED_TARGET GLOBAL aravis-0.10)

find_package(magic_enum CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)

target_link_libraries(video_source_plugin_library PRIVATE PkgConfig::aravis)
target_link_libraries(video_source_validation_plugin_library PRIVATE PkgConfig::aravis)
