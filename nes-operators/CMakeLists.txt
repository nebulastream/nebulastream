# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-operators NES_OPERATORS_SOURCE_FILES)

# Add Library
add_library(nes-operators ${NES_OPERATORS_SOURCE_FILES})
target_link_libraries(nes-operators PUBLIC nes-sources nes-window-types)
target_include_directories(nes-operators PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# Library containing OPC client
if (NES_USE_OPC)
    find_package(open62541 CONFIG REQUIRED)
    target_link_libraries(nes-operators PUBLIC open62541::open62541)
    add_compile_definitions(ENABLE_OPC_BUILD)
endif (NES_USE_OPC)

add_tests_if_enabled(tests)
