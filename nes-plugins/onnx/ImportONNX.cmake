
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
if (APPLE)
    if (NES_HOST_PROCESSOR MATCHES "x86_64")
        set(ONNX_TARGET "osx-x86_64")
    elseif (NES_HOST_PROCESSOR MATCHES "arm64")
        set(ONNX_TARGET "osx-arm64")
    endif ()
elseif (UNIX AND NOT APPLE)
    if (NES_HOST_PROCESSOR MATCHES "x86_64")
        set(ONNX_TARGET "linux-x64")
    elseif (NES_HOST_PROCESSOR MATCHES "arm64" OR NES_HOST_PROCESSOR MATCHES "aarch64")
        set(ONNX_TARGET "linux-aarch64")
    endif ()
else ()
    message(FATAL_ERROR "System not supported, currently we only support Linux and OSx")
endif ()

set(ONNX_BINARY_VERSION "v${ONNX_VERSION}")
set(ONNX_COMPRESSED_BINARY_NAME "onnxruntime-${ONNX_TARGET}-${ONNX_VERSION}.tgz")
set(ONNX_FOLDER_NAME "onnxruntime-${ONNX_TARGET}-${ONNX_VERSION}")
set(ONNX_COMPRESSED_FILE ${CMAKE_CURRENT_BINARY_DIR}/${ONNX_COMPRESSED_BINARY_NAME})
IF (NOT EXISTS ${ONNX_COMPRESSED_FILE})
    message(STATUS "ONNX binaries at ${CLANG_COMPRESSED_FILE} do not exist!")
    download_file(https://github.com/microsoft/onnxruntime/releases/download/${ONNX_BINARY_VERSION}/${ONNX_COMPRESSED_BINARY_NAME} ${ONNX_COMPRESSED_FILE}_tmp)
    file(RENAME ${ONNX_COMPRESSED_FILE}_tmp ${ONNX_COMPRESSED_FILE})
endif ()
IF (NOT EXISTS ${CMAKE_CURRENT_BINARY_DIR}/${ONNX_FOLDER_NAME})
    message(STATUS "Un-compress clang binaries!")
    file(ARCHIVE_EXTRACT INPUT ${ONNX_COMPRESSED_FILE} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
endif ()

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

