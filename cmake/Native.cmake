# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Enable native mtune/march for optimizations
option(NES_BUILD_NATIVE "Override mtune/march to load native support" OFF)

# Native/Generic march support
if (NES_BUILD_NATIVE)
    include(CheckCXXCompilerFlag)
    CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
    if (COMPILER_SUPPORTS_MARCH_NATIVE)
        message(STATUS "CMAKE detects native arch support")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mtune=native -march=native -DNES_BENCHMARKS_NATIVE_MODE=1")
        # AVX detection
        find_package(AVX)
        if (${AVX2_FOUND})
            message(STATUS "CMAKE detects AVX2 support")
            add_compile_definitions(HAS_AVX)
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2")
        endif ()
    else ()
        message(FATAL_ERROR "NES_BUILD_NATIVE was true but the compiler does not support it on this architecture.")
    endif ()
else ()
    # Compiler should produce specific code for system architecture
    if (CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
        message(STATUS "CMAKE detects generic x86_64 arch support")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=x86-64 -mtune=generic")
    elseif (APPLE AND CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
        message(STATUS "CMAKE detects APPLE ARM64 support")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcpu=apple-m1")
    elseif (NOT APPLE AND CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
        message(STATUS "CMAKE detects generic ARM64 support")
        # Arm themselves suggest using -mcpu=native, or in general,
        # to use -mcpu=CPU_TYPE. For more info, here:
        # https://community.arm.com/developer/tools-software/tools/b/tools-software-ides-blog/posts/compiler-flags-across-architectures-march-mtune-and-mcpu
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=armv8-a")
    else ()
        message(FATAL_ERROR "CMAKE_SYSTEM_PROCESSOR was ${CMAKE_SYSTEM_PROCESSOR} this is a unsupported architecture.")
    endif ()
endif ()
