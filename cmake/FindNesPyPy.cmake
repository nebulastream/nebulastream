# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Locates an installed pypy3 (nix devshell, apt pypy3/pypy3-dev, or any other install). A macro so each
# caller (nes-udf/bridge-pypy, nes-single-node-worker, nes-systests/systest) gets its own NES_PYPY_* vars.
# .nix/nix-cmake.sh runs under `nix develop --ignore-environment`, which drops PATH, so set
# NES_PYPY3_EXECUTABLE explicitly there (passed through by .nix/nix-run.sh).
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
        # The nix devshell sets PYTHONPATH and _PYTHON_SYSCONFIGDATA_NAME inside the shell itself, so
        # --ignore-environment can't strip them. Left alone, PyPy's sysconfig honors those and silently
        # imports CPython's sysconfigdata instead of its own -- so run pypy3 with a clean env.
        set(NES_PYPY_ENV_COMMAND ${CMAKE_COMMAND} -E env --unset=PYTHONPATH --unset=PYTHONHOME --unset=_PYTHON_SYSCONFIGDATA_NAME --)
        execute_process(
                COMMAND ${NES_PYPY_ENV_COMMAND} ${NES_PYPY_EXECUTABLE} -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
                OUTPUT_VARIABLE NES_PYPY_ABI_VERSION
                OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        execute_process(
                COMMAND ${NES_PYPY_ENV_COMMAND} ${NES_PYPY_EXECUTABLE} -c "import sysconfig; print(sysconfig.get_config_var('INCLUDEPY'))"
                OUTPUT_VARIABLE NES_PYPY_INCLUDE_DIR
                OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        execute_process(
                COMMAND ${NES_PYPY_ENV_COMMAND} ${NES_PYPY_EXECUTABLE} -c "import sys; print(sys.prefix)"
                OUTPUT_VARIABLE NES_PYPY_PREFIX
                OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        execute_process(
                COMMAND ${NES_PYPY_ENV_COMMAND} ${NES_PYPY_EXECUTABLE} -c "import sysconfig; print(sysconfig.get_config_var('MULTIARCH') or '')"
                OUTPUT_VARIABLE NES_PYPY_MULTIARCH
                OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        # PyPy's runtime .so location depends on packaging (nixpkgs: lib/, tarball: bin/, apt: the
        # multiarch triplet dir); our Nix-provided CMake doesn't set CMAKE_LIBRARY_ARCHITECTURE, so
        # hint at it explicitly via sysconfig's MULTIARCH.
        find_library(NES_PYPY_RUNTIME_LIB
                NAMES pypy${NES_PYPY_ABI_VERSION}-c
                HINTS ${NES_PYPY_PREFIX}/bin ${NES_PYPY_PREFIX}/lib ${NES_PYPY_PREFIX}/lib/${NES_PYPY_MULTIARCH})
        if (NES_PYPY_RUNTIME_LIB)
            set(NES_PYPY_FOUND TRUE)
        endif ()
    endif ()
endmacro()
