# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

option(USE_BUILD_CACHE "Use sccache or ccache to speed up rebuilds" ON)

if (NOT USE_BUILD_CACHE)
    message(STATUS "Compiler cache disabled")
    return()
endif ()

# Prefer sccache over ccache
find_program(SCCACHE_EXECUTABLE sccache)
find_program(CCACHE_EXECUTABLE ccache)

# When NES_BUILD_NATIVE is on, `-march=native` resolves per-host. A shared compiler cache
# keys on the literal command line and would otherwise hand back an object built for a
# richer ISA than the consumer has → SIGILL at runtime. Ask the clang driver what cc1
# target-cpu + target-feature flags it would actually pass for `-march=native` on this
# host, hash them, and feed the hash into the cache key via each compiler cache's
# documented "factor data into hash" knob:
#   sccache: SCCACHE_C_CUSTOM_CACHE_BUSTER (string value, README "Separating caches")
#   ccache:  CCACHE_EXTRAFILES              (file path, ccache.dev manual `extra_files_to_hash`)
# Delivered through a `cmake -E env` launcher prefix so CMAKE_CXX_FLAGS stays clean.
set(_NES_CACHE_ENV_PREFIX "")
if (NES_BUILD_NATIVE)
    execute_process(
            COMMAND "${CMAKE_CXX_COMPILER}" -march=native "-###" -x c -c /dev/null
            OUTPUT_QUIET
            ERROR_VARIABLE _clang_driver_dump
            RESULT_VARIABLE _probe_rc)
    set(_target_facts "")
    if (_probe_rc EQUAL 0)
        # Clang `-###` prints the cc1 invocation with each arg quoted, e.g.
        #   "-target-cpu" "skylake-avx512" "-target-feature" "+avx2" ...
        string(REGEX MATCHALL "\"-target-cpu\" \"[^\"]+\"" _cpu_args "${_clang_driver_dump}")
        string(REGEX MATCHALL "\"-target-feature\" \"[^\"]+\"" _feat_args "${_clang_driver_dump}")
        foreach (_arg IN LISTS _cpu_args _feat_args)
            string(REGEX REPLACE "\"(-target-(cpu|feature))\" \"([^\"]+)\"" "\\1=\\3" _arg "${_arg}")
            list(APPEND _target_facts "${_arg}")
        endforeach ()
        list(SORT _target_facts)
    endif ()
    if (_target_facts)
        string(REPLACE ";" "\n" _target_facts_text "${_target_facts}")
        set(_NES_CPU_FINGERPRINT_FILE "${CMAKE_BINARY_DIR}/cpu_fingerprint.txt")
        file(WRITE "${_NES_CPU_FINGERPRINT_FILE}" "${_target_facts_text}\n")
        string(SHA256 _cpu_fp_hash "${_target_facts_text}")
        string(SUBSTRING "${_cpu_fp_hash}" 0 16 _cpu_fp_hash)
        message(STATUS "Native CPU fingerprint: ${_cpu_fp_hash} (-> ${_NES_CPU_FINGERPRINT_FILE})")
        # Diagnostic: dump the full fact list so two builds on the same SKU can be confirmed
        # to produce identical fingerprints (i.e. no volatile non-CPU input is leaking in).
        foreach (_f IN LISTS _target_facts)
            message(STATUS "  fact: ${_f}")
        endforeach ()
        set(_NES_CACHE_ENV_PREFIX
                "${CMAKE_COMMAND};-E;env;SCCACHE_C_CUSTOM_CACHE_BUSTER=${_cpu_fp_hash};CCACHE_EXTRAFILES=${_NES_CPU_FINGERPRINT_FILE}")
    else ()
        # Fail closed: without a usable fingerprint a shared cache could hand back an
        # object compiled for an ISA the consumer doesn't have. Better a slow build than
        # a SIGILL at runtime.
        message(WARNING "NES_BUILD_NATIVE: could not extract -target-cpu/-target-feature from "
                "${CMAKE_CXX_COMPILER} -### output; disabling compiler cache to avoid "
                "cross-ISA cache pollution")
        set(USE_BUILD_CACHE OFF CACHE BOOL "Use sccache or ccache to speed up rebuilds" FORCE)
        return()
    endif ()
endif ()

if (SCCACHE_EXECUTABLE)
    message(STATUS "Using sccache: ${SCCACHE_EXECUTABLE}")
    set(CMAKE_CXX_COMPILER_LAUNCHER ${_NES_CACHE_ENV_PREFIX} "${SCCACHE_EXECUTABLE}")
    set(CMAKE_C_COMPILER_LAUNCHER ${_NES_CACHE_ENV_PREFIX} "${SCCACHE_EXECUTABLE}")
elseif (CCACHE_EXECUTABLE)
    message(STATUS "Using ccache: ${CCACHE_EXECUTABLE}")
    set(CMAKE_CXX_COMPILER_LAUNCHER ${_NES_CACHE_ENV_PREFIX} "${CCACHE_EXECUTABLE}")

    if (NES_ENABLE_PRECOMPILED_HEADERS)
        set(CMAKE_PCH_INSTANTIATE_TEMPLATES ON)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Xclang -fno-pch-timestamp")
        set(ENV{CCACHE_SLOPPINESS} "pch_defines,time_macros,include_file_ctime,include_file_mtime")
        message("CCACHE_SLOPPINESS: $ENV{CCACHE_SLOPPINESS}")
    endif ()
else ()
    message(STATUS "No compiler cache found (looked for sccache, ccache)")
endif ()
