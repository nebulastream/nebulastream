# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

option(USE_COMPILER_CACHE "Use sccache or ccache to speed up rebuilds" ON)

if (NOT USE_COMPILER_CACHE)
    message(STATUS "Compiler cache disabled")
    return()
endif ()

# Prefer sccache over ccache
find_program(SCCACHE_EXECUTABLE sccache)
find_program(CCACHE_EXECUTABLE ccache)

if (SCCACHE_EXECUTABLE)
    message(STATUS "Using sccache: ${SCCACHE_EXECUTABLE}")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${SCCACHE_EXECUTABLE}")
    set(CMAKE_C_COMPILER_LAUNCHER "${SCCACHE_EXECUTABLE}")
elseif (CCACHE_EXECUTABLE)
    message(STATUS "Using ccache: ${CCACHE_EXECUTABLE}")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_EXECUTABLE}")

    if (NES_ENABLE_PRECOMPILED_HEADERS)
        set(CMAKE_PCH_INSTANTIATE_TEMPLATES ON)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Xclang -fno-pch-timestamp")
        set(ENV{CCACHE_SLOPPINESS} "pch_defines,time_macros,include_file_ctime,include_file_mtime")
        message("CCACHE_SLOPPINESS: $ENV{CCACHE_SLOPPINESS}")
    endif ()
else ()
    message(STATUS "No compiler cache found (looked for sccache, ccache)")
endif ()
