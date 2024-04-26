
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
include(cmake/macros.cmake)

# NES supports self hosting its compilation with a pre-build clang
if (NES_SELF_HOSTING)
    set(LLVM_FOLDER_NAME nes-llvm-${LLVM_VERSION}-${LLVM_BINARY_VERSION})
    if (CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
        # Linux-specific stuff
        get_linux_lsb_release_information()
        message(STATUS "Linux ${LSB_RELEASE_ID_SHORT} ${LSB_RELEASE_VERSION_SHORT} ${LSB_RELEASE_CODENAME_SHORT}")
        set(NES_SUPPORTED_UBUNTU_VERSIONS 18.04 20.04 22.04)
        if ((NOT${LSB_RELEASE_ID_SHORT} STREQUAL "Ubuntu") OR (NOT ${LSB_RELEASE_VERSION_SHORT} IN_LIST NES_SUPPORTED_UBUNTU_VERSIONS))
            message(FATAL_ERROR "Currently we only provide pre-build dependencies for Ubuntu: ${NES_SUPPORTED_UBUNTU_VERSIONS}. If you use a different linux please provide an own clang installation.")
        endif ()
        if (NES_HOST_PROCESSOR MATCHES "x86_64")
            set(CLANG_COMPRESSED_BINARY_NAME nes-clang-${LLVM_VERSION}-ubuntu-${LSB_RELEASE_VERSION_SHORT}-x64)
        elseif (NES_HOST_PROCESSOR MATCHES "arm64" OR NES_HOST_PROCESSOR MATCHES "aarch64")
            set(CLANG_COMPRESSED_BINARY_NAME nes-clang-${LLVM_VERSION}-ubuntu-${LSB_RELEASE_VERSION_SHORT}-arm64)
        endif ()
    elseif (CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin")
        if (NES_HOST_PROCESSOR MATCHES "x86_64")
            set(CLANG_COMPRESSED_BINARY_NAME nes-clang-${LLVM_VERSION}-osx-x64)
        elseif (NES_HOST_PROCESSOR MATCHES "arm64" OR NES_HOST_PROCESSOR MATCHES "aarch64")
            set(CLANG_COMPRESSED_BINARY_NAME nes-clang-${LLVM_VERSION}-osx-arm64)
        endif ()
    else ()
        message(FATAL_ERROR "${CMAKE_SYSTEM_NAME} is not supported")
    endif ()

    cached_fetch_and_extract(
        https://github.com/nebulastream/clang-binaries/releases/download/${LLVM_BINARY_VERSION}/${CLANG_COMPRESSED_BINARY_NAME}.7z
        ${CMAKE_CURRENT_BINARY_DIR}/${LLVM_FOLDER_NAME}
   )

    message(STATUS "Self-host compilation of NES from ${LLVM_FOLDER_NAME}")
    # CMAKE_<LANG>_COMPILER are only set the first time a build tree is configured.
    # Setting it afterwards has no effect. It will be reset to a previously set value
    # when executing the PROJECT directive.
    # See: https://cmake.org/cmake/help/latest/variable/CMAKE_LANG_COMPILER.html
    set(CMAKE_C_COMPILER "${CMAKE_CURRENT_BINARY_DIR}/${LLVM_FOLDER_NAME}/clang/bin/clang")
    set(CMAKE_CXX_COMPILER "${CMAKE_CURRENT_BINARY_DIR}/${LLVM_FOLDER_NAME}/clang/bin/clang++")

    # Setup cmake configuration to include libs
    set(LLVM_DIR "${CMAKE_CURRENT_BINARY_DIR}/${LLVM_FOLDER_NAME}/clang/lib/cmake/llvm")
    set(MLIR_DIR "${CMAKE_CURRENT_BINARY_DIR}/${LLVM_FOLDER_NAME}/clang/lib/cmake/mlir")
    set(Clang_DIR "${CMAKE_CURRENT_BINARY_DIR}/${LLVM_FOLDER_NAME}/clang/lib/cmake/clang")
else ()
    message(STATUS "Use system compiler and local LLVM")
endif ()