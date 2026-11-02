# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Python UDFs: inline PYTHON(...) functions and catalog UDFs (CREATE FUNCTION ... BRIDGE '<bridge>') that Codon compiles into
# the query pipeline or the CPython/PyPy bridges interpret. Only part of the build with NES_ENABLE_PYTHON_UDFS; the generic
# UDF support (catalog, C-ABI runtime, UDF logical and physical functions) lives in nes-udf and the operator components.
find_package(codon-compiler CONFIG REQUIRED)
find_package(Python3 COMPONENTS Development.Embed QUIET)
include(${PROJECT_SOURCE_DIR}/cmake/FindNesPyPy.cmake)
nes_find_pypy()

add_subdirectory(codon-sdk)
add_subdirectory(codon-plugins/blas)
add_subdirectory(codon-plugins/opencv)
add_subdirectory(bridges)
add_subdirectory(functions)
