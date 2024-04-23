# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(NES_PLUGINS_BUILD_PATH "${CMAKE_BINARY_DIR}/nes-plugins")
set(NES_PLUGINS_INSTALL_PATH "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/nes-plugins")
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/PathConfig.h.in ${CMAKE_CURRENT_BINARY_DIR}/PathConfig.h)

target_include_directories(nes-common PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
        $<INSTALL_INTERFACE:include/nebulastream/>)

if (NES_BUILD_PLUGIN_ARROW AND CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT (CMAKE_SYSTEM_PROCESSOR MATCHES "arm" OR CMAKE_SYSTEM_PROCESSOR MATCHES "aarch"))
    add_subdirectory(arrow)
endif ()

if (NES_BUILD_PLUGIN_TENSORFLOW)
    add_subdirectory(tensorflow)
endif ()

if (NES_BUILD_PLUGIN_ONNX)
    add_subdirectory(onnx)
endif ()