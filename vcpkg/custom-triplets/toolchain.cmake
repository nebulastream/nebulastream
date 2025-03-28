# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This is a toolchain file for building dependencies and compiling NebulaStream
# 1. Enable the mold linker if it is available on the the system
# 2. Enable CCache if it is available on the system

if (NOT _NES_TOOLCHAIN_FILE)
    set(_NES_TOOLCHAIN_FILE 1)

    if (CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
        set(CMAKE_CROSSCOMPILING OFF CACHE BOOL "")
    endif ()
    if (VCPKG_TARGET_ARCHITECTURE STREQUAL "x64")
        set(CMAKE_SYSTEM_PROCESSOR x86_64 CACHE STRING "")
    endif ()
    set(CMAKE_SYSTEM_NAME Linux CACHE STRING "")

    find_program(MOLD_EXECUTABLE NAMES mold)
    set(LINK_WITH_MOLD "")
    if (MOLD_EXECUTABLE)
        set(LINK_WITH_MOLD "-fuse-ld=mold")
    endif ()

    find_program(CCACHE_EXECUTABLE ccache)
    if (CCACHE_EXECUTABLE)
        set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_EXECUTABLE}")
    endif ()

    # If clang is available we use clang and look for libc++
    find_program(CLANGXX_EXECUTABLE NAMES clang++ clang++-$ENV{LLVM_TOOLCHAIN_VERSION})
    find_program(CLANG_EXECUTABLE NAMES clang clang-$ENV{LLVM_TOOLCHAIN_VERSION})

    get_property(_CMAKE_IN_TRY_COMPILE GLOBAL PROPERTY IN_TRY_COMPILE)
    if (NOT _CMAKE_IN_TRY_COMPILE)
        string(APPEND CMAKE_C_FLAGS_INIT "-fPIC ${VCPKG_C_FLAGS} ")
        string(APPEND CMAKE_CXX_FLAGS_INIT "-std=c++23 -fPIC ${VCPKG_CXX_FLAGS} ")
        string(APPEND CMAKE_C_FLAGS_DEBUG_INIT "${VCPKG_C_FLAGS_DEBUG} ")
        string(APPEND CMAKE_CXX_FLAGS_DEBUG_INIT "${VCPKG_CXX_FLAGS_DEBUG} ")
        string(APPEND CMAKE_C_FLAGS_RELEASE_INIT "${VCPKG_C_FLAGS_RELEASE} ")
        string(APPEND CMAKE_CXX_FLAGS_RELEASE_INIT "${VCPKG_CXX_FLAGS_RELEASE} ")

        string(APPEND CMAKE_MODULE_LINKER_FLAGS_INIT "${LINK_WITH_MOLD} ${VCPKG_LINKER_FLAGS} ")
        string(APPEND CMAKE_SHARED_LINKER_FLAGS_INIT "${LINK_WITH_MOLD} ${VCPKG_LINKER_FLAGS} ")
        string(APPEND CMAKE_EXE_LINKER_FLAGS_INIT "${LINK_WITH_MOLD} ${VCPKG_LINKER_FLAGS} ")
        string(APPEND CMAKE_MODULE_LINKER_FLAGS_DEBUG_INIT "${VCPKG_LINKER_FLAGS_DEBUG} ")
        string(APPEND CMAKE_SHARED_LINKER_FLAGS_DEBUG_INIT "${VCPKG_LINKER_FLAGS_DEBUG} ")
        string(APPEND CMAKE_EXE_LINKER_FLAGS_DEBUG_INIT "${VCPKG_LINKER_FLAGS_DEBUG} ")
        string(APPEND CMAKE_MODULE_LINKER_FLAGS_RELEASE_INIT "${VCPKG_LINKER_FLAGS_RELEASE} ")
        string(APPEND CMAKE_SHARED_LINKER_FLAGS_RELEASE_INIT "${VCPKG_LINKER_FLAGS_RELEASE} ")
        string(APPEND CMAKE_EXE_LINKER_FLAGS_RELEASE_INIT "${VCPKG_LINKER_FLAGS_RELEASE} ")
    endif (NOT _CMAKE_IN_TRY_COMPILE)
endif (NOT _NES_TOOLCHAIN_FILE)
