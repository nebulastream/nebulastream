# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

option(USE_CCACHE_IF_AVAILABLE "Use ccache to speed up rebuilds" ON)
find_program(CCACHE_EXECUTABLE ccache)
if (CCACHE_EXECUTABLE AND ${USE_CCACHE_IF_AVAILABLE})
    message(STATUS "Using ccache: ${CCACHE_EXECUTABLE}")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_EXECUTABLE}")

    if (NES_ENABLE_PRECOMPILED_HEADERS)
        set(CMAKE_PCH_INSTANTIATE_TEMPLATES ON)
        # Need to set these to enable interplay between ccache and precompiled headers
        # https://ccache.dev/manual/4.8.html#_precompiled_headers
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Xclang -fno-pch-timestamp")
        set(ENV{CCACHE_SLOPPINESS} "pch_defines,time_macros,include_file_ctime,include_file_mtime")
        message("CCACHE_SLOPPINESS: $ENV{CCACHE_SLOPPINESS}")
    endif ()

else ()
    message(STATUS "Not using ccache")
endif ()
