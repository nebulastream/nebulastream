# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
add_plugin_as_library(RustFileCBG Source nes-sources-registry rust_file_cbg_source_plugin_library RustFileSourceCBG.cpp)
add_plugin_as_library(RustFileCBG SourceValidation nes-sources-registry rust_file_cbg_validation_plugin_library RustFileSourceCBG.cpp)
add_plugin_as_library(RustFileCBG InlineData nes-sources-registry rust_file_cbg_inline_data_plugin_library RustFileSourceCBG.cpp)
add_plugin_as_library(RustFileCBG FileData nes-sources-registry rust_file_cbg_file_data_plugin_library RustFileSourceCBG.cpp)
target_include_directories(rust_file_cbg_validation_plugin_library
        PRIVATE .
)

add_dependencies(rust_file_cbg_source_plugin_library rust_build_cbg)
add_dependencies(rust_file_cbg_validation_plugin_library rust_build_cbg)
add_dependencies(rust_file_cbg_inline_data_plugin_library rust_build_cbg)
add_dependencies(rust_file_cbg_file_data_plugin_library rust_build_cbg)
target_link_libraries(rust_file_cbg_source_plugin_library PUBLIC rust-lib-cbg)
