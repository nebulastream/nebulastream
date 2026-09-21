# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The PyPy UDF bridge is an optional runtime artifact (a .so users dlopen via CREATE FUNCTION), not
# linked into the engine. It embeds PyPy via cffi, so it only builds where pypy3 is installed, same
# as the CPython bridge next door.
include(${PROJECT_SOURCE_DIR}/cmake/FindNesPyPy.cmake)
nes_find_pypy()
if (NES_PYPY_FOUND)
    set(NES_PYPY_BRIDGE_GLUE ${CMAKE_CURRENT_SOURCE_DIR}/pypy_udf_bridge.py)
    set(NES_PYPY_BRIDGE_GENERATED_C ${CMAKE_CURRENT_BINARY_DIR}/nes_pypy_udf_bridge.c)

    # pypy_udf_bridge.py's cffi embedding step generates a .c file; we compile it like any other target.
    add_custom_command(
            OUTPUT ${NES_PYPY_BRIDGE_GENERATED_C}
            COMMAND ${NES_PYPY_EXECUTABLE} ${NES_PYPY_BRIDGE_GLUE} ${NES_PYPY_BRIDGE_GENERATED_C}
            DEPENDS ${NES_PYPY_BRIDGE_GLUE}
            COMMENT "Generating PyPy UDF bridge C source"
    )

    add_library(nes-pypy-udf-bridge SHARED ${NES_PYPY_BRIDGE_GENERATED_C})
    target_include_directories(nes-pypy-udf-bridge PRIVATE ${NES_PYPY_INCLUDE_DIR})
    target_link_libraries(nes-pypy-udf-bridge PRIVATE ${NES_PYPY_RUNTIME_LIB})

    # The systest binary (configured before this dir) dlopens the bridge; ensure it is built alongside.
    if (TARGET systest)
        add_dependencies(systest nes-pypy-udf-bridge)
    endif ()
    # See nes-udf/bridge-cpython/CMakeLists.txt for why the worker-side staging step lives in
    # nes-single-node-worker/CMakeLists.txt instead of here.
    if (TARGET nes-single-node-worker)
        add_dependencies(nes-single-node-worker nes-pypy-udf-bridge)
    endif ()

    install(TARGETS nes-pypy-udf-bridge
            RUNTIME DESTINATION bin/nes-udf-bridges
            LIBRARY DESTINATION bin/nes-udf-bridges)
    message(STATUS "PyPy UDF bridge enabled (PyPy ${NES_PYPY_ABI_VERSION}, ${NES_PYPY_EXECUTABLE})")
else ()
    message(STATUS "PyPy UDF bridge disabled (pypy3 not found)")
endif ()
