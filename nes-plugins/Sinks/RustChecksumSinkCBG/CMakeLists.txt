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
add_plugin_as_library(RustChecksumCBG Sink nes-sinks-registry rust_checksum_sink_cbg_plugin RustChecksumSinkCBG.cpp)
add_plugin_as_library(RustChecksumCBG SinkValidation nes-sinks-registry rust_checksum_sink_cbg_validation_plugin RustChecksumSinkCBG.cpp)

add_dependencies(rust_checksum_sink_cbg_plugin rust_build_cbg)
add_dependencies(rust_checksum_sink_cbg_validation_plugin rust_build_cbg)
target_link_libraries(rust_checksum_sink_cbg_plugin PRIVATE rust-lib-cbg)
