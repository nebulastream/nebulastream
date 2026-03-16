# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(src)
get_source(nes-store-manager NES_STORE_MANAGER_SOURCE_FILES)

add_library(nes-store-manager ${NES_STORE_MANAGER_SOURCE_FILES})

target_link_libraries(nes-store-manager PUBLIC nes-common nes-data-types)
target_compile_definitions(nes-store-manager PRIVATE STORE_MANAGER_WORKING_DIR="${CMAKE_CURRENT_BINARY_DIR}/working-dir/")
target_include_directories(nes-store-manager PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-store-manager REUSE_FROM nes-common)
endif ()
