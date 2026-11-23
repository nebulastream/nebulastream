# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The CPython UDF bridge is an optional runtime artifact (a .so users dlopen via CREATE FUNCTION),
# not linked into the engine. It embeds CPython, so it only builds where the Python "embed"
# development files are available.
if (TARGET Python3::Python)
    add_library(nes-cpython-udf-bridge SHARED CPythonUdfBridge.cpp)
    target_include_directories(nes-cpython-udf-bridge PRIVATE ${PROJECT_SOURCE_DIR}/nes-udf/include)
    target_link_libraries(nes-cpython-udf-bridge PRIVATE Python3::Python)
    # Sanitizer runtimes are linked into executables only; the worker that dlopens the bridge provides them.
    if (NOT SANITIZER_OPTION STREQUAL "none")
        target_link_options(nes-cpython-udf-bridge PRIVATE "LINKER:-z,undefs")
    endif ()
    # nes-single-node-worker and nes-systests/systest stage the bridge next to their executables.
    install(TARGETS nes-cpython-udf-bridge
            RUNTIME DESTINATION bin/nes-udf-bridges
            LIBRARY DESTINATION bin/nes-udf-bridges)
    message(STATUS "CPython UDF bridge enabled (Python ${Python3_VERSION})")
    # The systests build a venv for the UDF process with this Python, so the bridge can share it.
    set(NES_CPYTHON_UDF_BRIDGE_PYTHON_VERSION "${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}" CACHE INTERNAL "")
else ()
    message(STATUS "CPython UDF bridge disabled (Python Development.Embed not found)")
endif ()

# The same bridge, built against a free-threaded CPython (PEP 703, python3.14t), which has no GIL. Its ABI differs from the
# default build, so it is a second library. The worker loads it for in_process_udf_interpreter=FREE_THREADED, from the same
# directory as the default bridge. NES_CPYTHON_FREE_THREADED_EXECUTABLE selects the interpreter, e.g. from `uv python find 3.14t`.
find_program(NES_CPYTHON_FREE_THREADED_EXECUTABLE NAMES python3.14t python3.13t HINTS "$ENV{HOME}/.local/bin"
        DOC "Free-threaded CPython whose libpython the free-threaded UDF bridge embeds")
if (NES_CPYTHON_FREE_THREADED_EXECUTABLE)
    execute_process(
            COMMAND "${NES_CPYTHON_FREE_THREADED_EXECUTABLE}" -I -c
            "import sysconfig as s; print(s.get_config_var('Py_GIL_DISABLED') or 0, s.get_config_var('INCLUDEPY'), s.get_config_var('LIBDIR'), s.get_config_var('LDLIBRARY'), sep=';', end='')"
            OUTPUT_VARIABLE NES_CPYTHON_FT_CONFIG
            RESULT_VARIABLE NES_CPYTHON_FT_RESULT)
    list(LENGTH NES_CPYTHON_FT_CONFIG NES_CPYTHON_FT_CONFIG_LENGTH)
    if (NES_CPYTHON_FT_RESULT EQUAL 0 AND NES_CPYTHON_FT_CONFIG_LENGTH EQUAL 4)
        list(GET NES_CPYTHON_FT_CONFIG 0 NES_CPYTHON_FT_GIL_DISABLED)
        list(GET NES_CPYTHON_FT_CONFIG 1 NES_CPYTHON_FT_INCLUDE)
        list(GET NES_CPYTHON_FT_CONFIG 2 NES_CPYTHON_FT_LIBDIR)
        list(GET NES_CPYTHON_FT_CONFIG 3 NES_CPYTHON_FT_LIBRARY)
    endif ()
    if (NES_CPYTHON_FT_GIL_DISABLED EQUAL 1 AND EXISTS "${NES_CPYTHON_FT_LIBDIR}/${NES_CPYTHON_FT_LIBRARY}")
        add_library(nes-cpython-udf-bridge-ft SHARED CPythonUdfBridge.cpp)
        target_include_directories(nes-cpython-udf-bridge-ft PRIVATE ${PROJECT_SOURCE_DIR}/nes-udf/include)
        target_include_directories(nes-cpython-udf-bridge-ft SYSTEM PRIVATE "${NES_CPYTHON_FT_INCLUDE}")
        target_link_libraries(nes-cpython-udf-bridge-ft PRIVATE "${NES_CPYTHON_FT_LIBDIR}/${NES_CPYTHON_FT_LIBRARY}")
        set_target_properties(nes-cpython-udf-bridge-ft PROPERTIES BUILD_RPATH "${NES_CPYTHON_FT_LIBDIR}" INSTALL_RPATH "${NES_CPYTHON_FT_LIBDIR}")
        if (NOT SANITIZER_OPTION STREQUAL "none")
            target_link_options(nes-cpython-udf-bridge-ft PRIVATE "LINKER:-z,undefs")
        endif ()
        install(TARGETS nes-cpython-udf-bridge-ft
                RUNTIME DESTINATION bin/nes-udf-bridges
                LIBRARY DESTINATION bin/nes-udf-bridges)
        message(STATUS "Free-threaded CPython UDF bridge enabled (${NES_CPYTHON_FREE_THREADED_EXECUTABLE})")
    else ()
        message(STATUS "Free-threaded CPython UDF bridge disabled (${NES_CPYTHON_FREE_THREADED_EXECUTABLE} is not a free-threaded build)")
    endif ()
endif ()
