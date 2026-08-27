# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Locates an installed pypy3 (nix devshell, apt's pypy3/pypy3-dev, or any other install -- same
# "just find it, no CMake option" contract as Python3's Development.Embed). A macro, not a function,
# so each of nes-udf/bridge-pypy, nes-single-node-worker, and nes-systests/systest can call it and get
# its own NES_PYPY_* variables in their own scope, mirroring how each of them independently re-runs
# find_package(Python3 COMPONENTS Development.Embed QUIET) rather than sharing one result.
#
# .nix/nix-cmake.sh builds inside a `nix develop --ignore-environment` shell, which does not inherit
# PATH -- a system-installed pypy3 is invisible to a plain find_program there. NES_PYPY3_EXECUTABLE
# (passed through the isolation by .nix/nix-run.sh's KEEP_VARS, same as SCCACHE_*/AWS_*) lets it
# through explicitly instead of widening the sandbox.
#
# Sets NES_PYPY_FOUND, and on success NES_PYPY_EXECUTABLE / NES_PYPY_RUNTIME_LIB / NES_PYPY_INCLUDE_DIR.
macro(nes_find_pypy)
    if (DEFINED ENV{NES_PYPY3_EXECUTABLE} AND NOT "$ENV{NES_PYPY3_EXECUTABLE}" STREQUAL "")
        set(NES_PYPY_EXECUTABLE "$ENV{NES_PYPY3_EXECUTABLE}" CACHE FILEPATH "Path to pypy3 (from NES_PYPY3_EXECUTABLE)" FORCE)
    else ()
        find_program(NES_PYPY_EXECUTABLE pypy3)
    endif ()
    set(NES_PYPY_FOUND FALSE)
    if (NES_PYPY_EXECUTABLE AND NOT EXISTS "${NES_PYPY_EXECUTABLE}")
        message(WARNING "NES_PYPY3_EXECUTABLE=${NES_PYPY_EXECUTABLE} does not exist; PyPy UDF bridge disabled")
        set(NES_PYPY_EXECUTABLE "NES_PYPY_EXECUTABLE-NOTFOUND")
    endif ()
    if (NES_PYPY_EXECUTABLE)
        execute_process(
                COMMAND ${NES_PYPY_EXECUTABLE} -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
                OUTPUT_VARIABLE NES_PYPY_ABI_VERSION
                OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        execute_process(
                COMMAND ${NES_PYPY_EXECUTABLE} -c "import sysconfig; print(sysconfig.get_config_var('INCLUDEPY'))"
                OUTPUT_VARIABLE NES_PYPY_INCLUDE_DIR
                OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        execute_process(
                COMMAND ${NES_PYPY_EXECUTABLE} -c "import sys; print(sys.prefix)"
                OUTPUT_VARIABLE NES_PYPY_PREFIX
                OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        # The runtime .so's directory differs by how PyPy was packaged (nixpkgs: lib/, the official
        # pypy.org tarball: bin/) -- search both instead of assuming one.
        find_library(NES_PYPY_RUNTIME_LIB
                NAMES pypy${NES_PYPY_ABI_VERSION}-c
                PATHS ${NES_PYPY_PREFIX}/bin ${NES_PYPY_PREFIX}/lib
                NO_DEFAULT_PATH)
        if (NES_PYPY_RUNTIME_LIB)
            set(NES_PYPY_FOUND TRUE)
        endif ()
    endif ()
endmacro()
