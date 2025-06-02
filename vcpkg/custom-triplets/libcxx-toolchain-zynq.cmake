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
# 3. Use Libc++ if available

if (NOT _NES_TOOLCHAIN_FILE)
    set(_NES_TOOLCHAIN_FILE 1)

    if (CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
        #set(CMAKE_CROSSCOMPILING OFF CACHE BOOL "")
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
    find_program(CLANGXX_EXECUTABLE REQUIRED NAMES clang++-$ENV{LLVM_TOOLCHAIN_VERSION})
    find_program(CLANG_EXECUTABLE REQUIRED NAMES clang-$ENV{LLVM_TOOLCHAIN_VERSION})
    set(LIBCXX_COMPILER_FLAG "-stdlib=libc++")
    set(LIBCXX_LINKER_FLAG "-lc++")
    set(CMAKE_CXX_COMPILER ${CLANGXX_EXECUTABLE})
    set(CMAKE_C_COMPILER ${CLANG_EXECUTABLE})

    get_property(_CMAKE_IN_TRY_COMPILE GLOBAL PROPERTY IN_TRY_COMPILE)
    if (NOT _CMAKE_IN_TRY_COMPILE)
        string(APPEND CMAKE_C_FLAGS_INIT "-fPIC ${VCPKG_C_FLAGS} ")
        string(APPEND CMAKE_CXX_FLAGS_INIT "-std=c++23 ${LIBCXX_COMPILER_FLAG} -fPIC ${VCPKG_CXX_FLAGS} ")
        string(APPEND CMAKE_C_FLAGS_DEBUG_INIT "${VCPKG_C_FLAGS_DEBUG} ")
        string(APPEND CMAKE_CXX_FLAGS_DEBUG_INIT "${VCPKG_CXX_FLAGS_DEBUG} ")
        string(APPEND CMAKE_C_FLAGS_RELEASE_INIT "${VCPKG_C_FLAGS_RELEASE} ")
        string(APPEND CMAKE_CXX_FLAGS_RELEASE_INIT "${VCPKG_CXX_FLAGS_RELEASE} ")

        string(APPEND CMAKE_MODULE_LINKER_FLAGS_INIT "${LIBCXX_LINKER_FLAG} ${LINK_WITH_MOLD} ${VCPKG_LINKER_FLAGS} ")
        string(APPEND CMAKE_SHARED_LINKER_FLAGS_INIT "${LIBCXX_LINKER_FLAG} ${LINK_WITH_MOLD} ${VCPKG_LINKER_FLAGS} ")
        string(APPEND CMAKE_EXE_LINKER_FLAGS_INIT "${LIBCXX_LINKER_FLAG} ${LINK_WITH_MOLD} ${VCPKG_LINKER_FLAGS} ")
        string(APPEND CMAKE_MODULE_LINKER_FLAGS_DEBUG_INIT "${VCPKG_LINKER_FLAGS_DEBUG} ")
        string(APPEND CMAKE_SHARED_LINKER_FLAGS_DEBUG_INIT "${VCPKG_LINKER_FLAGS_DEBUG} ")
        string(APPEND CMAKE_EXE_LINKER_FLAGS_DEBUG_INIT "${VCPKG_LINKER_FLAGS_DEBUG} ")
        string(APPEND CMAKE_MODULE_LINKER_FLAGS_RELEASE_INIT "${VCPKG_LINKER_FLAGS_RELEASE} ")
        string(APPEND CMAKE_SHARED_LINKER_FLAGS_RELEASE_INIT "${VCPKG_LINKER_FLAGS_RELEASE} ")
        string(APPEND CMAKE_EXE_LINKER_FLAGS_RELEASE_INIT "${VCPKG_LINKER_FLAGS_RELEASE} ")
    endif (NOT _CMAKE_IN_TRY_COMPILE)

    # aarch64-toolchain.cmake
    set(CMAKE_SYSTEM_NAME Linux)
    set(CMAKE_SYSTEM_PROCESSOR aarch64)

    # Specify the cross-compiler
    set(CMAKE_C_COMPILER clang)
    set(CMAKE_CXX_COMPILER clang++)

    # Specify the target flags for Clang
    set(CMAKE_C_FLAGS "--target=aarch64-linux-gnu" CACHE STRING "C compiler flags")
    set(CMAKE_CXX_FLAGS "--target=aarch64-linux-gnu" CACHE STRING "C++ compiler flags")

    # Specify the target environment (sysroot)
    set(CMAKE_FIND_ROOT_PATH /data/ntantow/ESPAT/zcu106_sysroot)
    set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
    set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
    set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
    set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)

    # Set cache variables for try_run tests
    set(FOLLY_HAVE_UNALIGNED_ACCESS_EXITCODE 0 CACHE INTERNAL "")
    set(FOLLY_HAVE_WEAK_SYMBOLS_EXITCODE 0 CACHE INTERNAL "")
    set(FOLLY_HAVE_LINUX_VDSO_EXITCODE 0 CACHE INTERNAL "")
    set(FOLLY_HAVE_WCHAR_SUPPORT_EXITCODE 0 CACHE INTERNAL "")
    set(HAVE_VSNPRINTF_ERRORS_EXITCODE 0 CACHE INTERNAL "")

endif (NOT _NES_TOOLCHAIN_FILE)
