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
