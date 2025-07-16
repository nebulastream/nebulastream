# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
add_plugin_as_library(RustFileCXX Source nes-sources-registry rust_file_cxx_source_plugin_library RustFileSourceCXX.cpp)
add_plugin_as_library(RustFileCXX SourceValidation nes-sources-registry rust_file_cxx_validation_plugin_library RustFileSourceCXX.cpp)
add_plugin_as_library(RustFileCXX InlineData nes-sources-registry rust_file_cxx_inline_data_plugin_library RustFileSourceCXX.cpp)
add_plugin_as_library(RustFileCXX FileData nes-sources-registry rust_file_cxx_file_data_plugin_library RustFileSourceCXX.cpp)
target_include_directories(rust_file_cxx_validation_plugin_library PRIVATE .)

add_dependencies(rust_file_cxx_source_plugin_library rust_build_cxx)
add_dependencies(rust_file_cxx_validation_plugin_library rust_build_cxx)
add_dependencies(rust_file_cxx_inline_data_plugin_library rust_build_cxx)
add_dependencies(rust_file_cxx_file_data_plugin_library rust_build_cxx)
target_link_libraries(rust_file_cxx_source_plugin_library PUBLIC rust-lib-cxx)
