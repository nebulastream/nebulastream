
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(FetchContent)
include(../../cmake/macros.cmake)
if (CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin")
    set(ONNX_TARGET osx-${CMAKE_HOST_SYSTEM_PROCESSOR})
elseif ((CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux") AND ((CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "arm64") OR (CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "aarch64")))
    set(ONNX_TARGET "linux-aarch64")
elseif ((CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux") AND (CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "x86_64"))
    set(ONNX_TARGET "linux-x64")
else ()
    message(FATAL_ERROR "System not supported, currently we only support Linux and OSx")
endif ()

set(ONNX_BINARY_VERSION "v${ONNX_VERSION}")
set(ONNX_COMPRESSED_BINARY_NAME "onnxruntime-${ONNX_TARGET}-${ONNX_VERSION}.tgz")
set(ONNX_FOLDER_NAME "onnxruntime-${ONNX_TARGET}-${ONNX_VERSION}")
set(ONNX_COMPRESSED_FILE ${CMAKE_CURRENT_BINARY_DIR}/${ONNX_COMPRESSED_BINARY_NAME})

cached_fetch_and_extract(
    https://github.com/microsoft/onnxruntime/releases/download/${ONNX_BINARY_VERSION}/${ONNX_COMPRESSED_BINARY_NAME}
    ${CMAKE_CURRENT_BINARY_DIR}/${ONNX_FOLDER_NAME}
)

set(onnxruntime_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/${ONNX_FOLDER_NAME}/include)
set(onnxruntime_LIBRARIES onnxruntime)
set(onnxruntime_CXX_FLAGS "") # no flags needed

find_library(onnxruntime_LIBRARY onnxruntime
        PATHS "${CMAKE_CURRENT_BINARY_DIR}/${ONNX_FOLDER_NAME}/lib"
)
add_library(onnxruntime SHARED IMPORTED)
# find_library returns the unversioned filename, which is a symlink to the versioned filename.
# However, we need to install the versioned filename in the Debian package; otherwise, we install a broken symlink.
# See: https://gitlab.kitware.com/cmake/cmake/-/issues/23249
get_filename_component(onxxruntime_LIBRARY ${onnxruntime_LIBRARY} REALPATH)
set_property(TARGET onnxruntime PROPERTY IMPORTED_LOCATION "${onxxruntime_LIBRARY}")
set_property(TARGET onnxruntime PROPERTY INTERFACE_INCLUDE_DIRECTORIES "${onnxruntime_INCLUDE_DIRS}")
set_property(TARGET onnxruntime PROPERTY INTERFACE_COMPILE_OPTIONS "${onnxruntime_CXX_FLAGS}")

find_package_handle_standard_args(onnxruntime DEFAULT_MSG onnxruntime_LIBRARY onnxruntime_INCLUDE_DIRS)

