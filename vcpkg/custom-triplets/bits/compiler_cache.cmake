# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Compiler cache configuration - try sccache first, then ccache
# Can be overridden with NES_USE_COMPILER_CACHE environment variable
set(USE_COMPILER_CACHE "$ENV{NES_USE_COMPILER_CACHE}")
if (NOT USE_COMPILER_CACHE)
    set(USE_COMPILER_CACHE "auto")
endif ()

set(COMPILER_CACHE_EXECUTABLE "")

if (USE_COMPILER_CACHE STREQUAL "auto")
    find_program(SCCACHE_EXECUTABLE sccache)
    if (SCCACHE_EXECUTABLE)
        set(COMPILER_CACHE_EXECUTABLE "${SCCACHE_EXECUTABLE}")
    else ()
        find_program(CCACHE_EXECUTABLE ccache)
        if (CCACHE_EXECUTABLE)
            set(COMPILER_CACHE_EXECUTABLE "${CCACHE_EXECUTABLE}")
        endif ()
    endif ()
elseif (USE_COMPILER_CACHE STREQUAL "sccache")
    find_program(SCCACHE_EXECUTABLE sccache)
    if (NOT SCCACHE_EXECUTABLE)
        message(FATAL_ERROR "sccache was requested but not found on the system")
    endif ()
    set(COMPILER_CACHE_EXECUTABLE "${SCCACHE_EXECUTABLE}")
elseif (USE_COMPILER_CACHE STREQUAL "ccache")
    find_program(CCACHE_EXECUTABLE ccache)
    if (NOT CCACHE_EXECUTABLE)
        message(FATAL_ERROR "ccache was requested but not found on the system")
    endif ()
    set(COMPILER_CACHE_EXECUTABLE "${CCACHE_EXECUTABLE}")
endif ()

if (COMPILER_CACHE_EXECUTABLE)
    set(CMAKE_CXX_COMPILER_LAUNCHER "${COMPILER_CACHE_EXECUTABLE}")
endif ()
