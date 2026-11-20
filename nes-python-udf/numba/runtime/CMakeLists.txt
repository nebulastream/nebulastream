# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The runtime that code compiled by Numba calls (COMPILER NUMBA): Numba's own C files (third_party/numba, unchanged, BSD
# license), the helpers of NumbaRuntimeHelpers.c, and the symbol table the worker gives the JIT (NumbaRuntime.cpp). No
# Python: python-shim/Python.h provides the two types Numba's headers take from CPython.
add_library(nes-numba-runtime STATIC
        NumbaRuntime.cpp
        NumbaRuntimeHelpers.c
        third_party/numba/core/runtime/nrt.cpp
        third_party/numba/cext/dictobject.c
        third_party/numba/cext/listobject.c
        third_party/numba/cext/utils.c)
target_include_directories(nes-numba-runtime PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/third_party/numba ${CMAKE_CURRENT_SOURCE_DIR}/third_party/numba/cext
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/numba/core/runtime ${CMAKE_CURRENT_SOURCE_DIR}/python-shim)
target_link_libraries(nes-numba-runtime PRIVATE nes-common nes-python-udf-compiler)
# Hidden: the worker exports its symbols, and the embedded CPython of IN_PROCESS UDFs, or Numba's own extension modules there,
# must not bind to these. The JIT reaches them through getNumbaRuntimeSymbols.
target_compile_options(nes-numba-runtime PRIVATE -fvisibility=hidden)
# Numba's files and the C helpers are not held to NES's warnings.
set_source_files_properties(NumbaRuntimeHelpers.c third_party/numba/core/runtime/nrt.cpp third_party/numba/cext/dictobject.c
        third_party/numba/cext/listobject.c third_party/numba/cext/utils.c PROPERTIES COMPILE_OPTIONS "-w")
