
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

# We rely on VCPKG for external dependencies.
# In this script we set up VCPKG depending on the local configuration and system.
# In general, we support three configurations:
# 1. Pre-build dependencies:
#   In this case, we download a archive, which contains all dependencies for NebulaStream.
#   Currently, we provide pre-build dependencies for Ubuntu 20.04 and OSx both for x64 and arm64.
#   If you use another system, the pre-build dependencies could cause undefined errors.
# 2. Local-build dependencies:
#   In this case, we build all dependencies locally as part in the build process.
#   Depending on you local system this may take a while, in particular building clang takes a while.
# 3. Provided dependencies:
#   In this case you provide an own vcpkg toolchain file, such that we use that one to load the dependencies.
#   Please clone https://github.com/nebulastream/nebulastream-dependencies and
#   provide the path to the "scripts/buildsystems/vcpkg.cmake" file as -DCMAKE_TOOLCHAIN_FILE=

# Identify the VCPKG_TARGET_TRIPLET depending on the architecture and operating system
#
# The system architecture is normally set in CMAKE_HOST_SYSTEM_PROCESSOR,
# which is set by the PROJECT command. However, we cannot call PROJECT
# at this point because we want to use a custom toolchain file.
execute_process(COMMAND uname -m OUTPUT_VARIABLE NES_HOST_PROCESSOR)
if (NES_HOST_PROCESSOR MATCHES "x86_64")
    set(NES_HOST_PROCESSOR "x64")
elseif (NES_HOST_PROCESSOR MATCHES "arm64" OR NES_HOST_PROCESSOR MATCHES "aarch64")
    set(NES_HOST_PROCESSOR "arm64")
else ()
    message(FATAL_ERROR "Only x86_64 and arm64 supported")
endif ()

execute_process(COMMAND uname -s OUTPUT_VARIABLE NES_HOST_NAME)
if (NES_HOST_NAME MATCHES "Linux")
    set(NES_HOST_NAME "linux")
elseif (NES_HOST_NAME MATCHES "Darwin")
    set(NES_HOST_NAME "osx")
else ()
    message(FATAL_ERROR "Only Linux and OS X supported")
endif ()

set(VCPKG_TARGET_TRIPLET ${NES_HOST_PROCESSOR}-${NES_HOST_NAME}-nes)
message(STATUS "Use VCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}")

# Ensure linux users without a custom toolchain file or using self hosting use a supported distribution
get_linux_lsb_release_information()
if (NES_HOST_NAME STREQUAL "linux" AND NOT CMAKE_TOOLCHAIN_FILE AND NES_SELF_HOSTING)
    message(STATUS "Linux ${LSB_RELEASE_ID_SHORT} ${LSB_RELEASE_VERSION_SHORT} ${LSB_RELEASE_CODENAME_SHORT}")
    set(NES_SUPPORTED_UBUNTU_VERSIONS 20.04 22.04 24.04)
    if ((NOT${LSB_RELEASE_ID_SHORT} STREQUAL "Ubuntu") OR (NOT ${LSB_RELEASE_VERSION_SHORT} IN_LIST NES_SUPPORTED_UBUNTU_VERSIONS))
        message(FATAL_ERROR "Currently we only provide pre-build dependencies and compiler toolchain for Ubuntu: ${NES_SUPPORTED_UBUNTU_VERSIONS}. If you use a different linux please provide your own dependencies and clang installation.")
    endif ()
endif ()

# In the following we configure vcpkg depending on the selected configuration.
if (CMAKE_TOOLCHAIN_FILE)
    # If a custom toolchain file is provided we use these dependencies.
    # To this end, we have to set the correct NES_DEPENDENCIES_BINARY_ROOT.
    message(STATUS "Use provided dependencies with toolchain file: ${CMAKE_TOOLCHAIN_FILE}.")
    cmake_path(GET CMAKE_TOOLCHAIN_FILE PARENT_PATH ParentPath)
    cmake_path(GET ParentPath PARENT_PATH ParentPath)
    cmake_path(GET ParentPath PARENT_PATH ParentPath)
    set(NES_DEPENDENCIES_BINARY_ROOT ${ParentPath}/installed/${VCPKG_TARGET_TRIPLET})
elseif (NES_BUILD_DEPENDENCIES_LOCAL)
    # Build all dependencies locally.
    # To this end, we check out the nebulastream-dependencies repository and use its manifest file.
    message(STATUS "Build Dependencies locally. This may take a while.")
    FetchContent_Declare(
            nesdebs
            GIT_REPOSITORY https://github.com/nebulastream/nebulastream-dependencies.git
            GIT_TAG ${VCPKG_BINARY_VERSION}
    )
    FetchContent_Populate(nesdebs)
    set(CMAKE_TOOLCHAIN_FILE ${nesdebs_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake
            CACHE STRING "CMake toolchain file")
    set(VCPKG_MANIFEST_DIR ${nesdebs_SOURCE_DIR} CACHE STRING "vcpkg manifest dir")
    set(VCPKG_OVERLAY_TRIPLETS ${nesdebs_SOURCE_DIR}/custom-triplets/ CACHE STRING "CMake toolchain file")
    set(VCPKG_OVERLAY_PORTS ${nesdebs_SOURCE_DIR}/vcpkg-registry/ports CACHE STRING "VCPKG overlay ports")
    set(NES_DEPENDENCIES_BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/${BINARY_NAME}/vcpkg_installed/${VCPKG_TARGET_TRIPLET})
else (NES_USE_PREBUILD_DEPENDENCIES)
    # Use the prebuild dependencies. To this end, we download the correct dependency file from our repository.
    message(STATUS "Use prebuild dependencies")
    set(BINARY_NAME nes-dependencies-${VCPKG_BINARY_VERSION}-${VCPKG_TARGET_TRIPLET})

    if (NES_HOST_NAME STREQUAL "linux")
        set(COMPRESSED_BINARY_NAME nes-dependencies-${VCPKG_BINARY_VERSION}-${NES_HOST_PROCESSOR}-linux-ubuntu-${LSB_RELEASE_VERSION_SHORT}-nes)
    elseif (NES_HOST_NAME STREQUAL "osx")
        set(COMPRESSED_BINARY_NAME ${BINARY_NAME})
    else ()
        message(FATAL_ERROR "pre-built dependencies exist only for Ubuntu/OS X and x86/arm64")
    endif ()

    cached_fetch_and_extract(
            https://github.com/nebulastream/dependencies/releases/download/${VCPKG_BINARY_VERSION}/${COMPRESSED_BINARY_NAME}.7z
            ${CMAKE_CURRENT_BINARY_DIR}/${BINARY_NAME}
    )

    # Set toolchain file to use prebuild dependencies.
    message(STATUS "Set toolchain file for prebuild dir.")
    set(CMAKE_TOOLCHAIN_FILE "${CMAKE_CURRENT_BINARY_DIR}/${BINARY_NAME}/scripts/buildsystems/vcpkg.cmake")
    set(NES_DEPENDENCIES_BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/${BINARY_NAME}/installed/${VCPKG_TARGET_TRIPLET})
endif ()
message(STATUS "NES_DEPENDENCIES_BINARY_ROOT: ${NES_DEPENDENCIES_BINARY_ROOT}.")

if (NES_SELF_HOSTING)
    set(LLVM_FOLDER_NAME nes-llvm-${LLVM_VERSION}-${LLVM_BINARY_VERSION})
    if (NES_HOST_NAME STREQUAL "linux")
        set(CLANG_COMPRESSED_BINARY_NAME nes-clang-${LLVM_VERSION}-ubuntu-${LSB_RELEASE_VERSION_SHORT}-${NES_HOST_PROCESSOR})
    elseif (NES_HOST_NAME STREQUAL "osx")
        set(CLANG_COMPRESSED_BINARY_NAME nes-clang-${LLVM_VERSION}-osx-${NES_HOST_PROCESSOR})
    else ()
        message(FATAL_ERROR "Pre-built LLVM exists only for Ubuntu/OS X and x64/arm64")
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

unset(NES_HOST_NAME)
unset(NES_HOST_PROCESSOR)
