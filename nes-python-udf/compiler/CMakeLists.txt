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

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)

# The Codon compiler of Inline Python UDFs. The UDF catalog runs it when a UDF is registered (UdfCompiler registry), so
# every binary with a UDF catalog links Codon; the worker runs it only for UDFs that do not come from the catalog.
add_library(nes-python-udf-compiler STATIC
        CodonUdfCompiler.cpp
        HiPyUdfCompiler.cpp
        NumbaUdfCompiler.cpp
        PythonUdfCompiler.cpp
        PythonUdfFilesystem.cpp)
target_include_directories(nes-python-udf-compiler PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
find_package(nlohmann_json CONFIG REQUIRED)
target_link_libraries(nes-python-udf-compiler PUBLIC nes-udf PRIVATE nes-codon-plugin codon::compiler nlohmann_json::nlohmann_json)
add_dependencies(nes-python-udf-compiler nes-codon-blas-plugin nes-codon-opencv-plugin nes-codon-compat-plugin nes-codon-json-plugin
        nes-codon-re-plugin nes-codon-unicode-plugin)
target_compile_definitions(nes-python-udf-compiler PRIVATE
        NES_CODON_BLAS_PLUGIN_PATH="$<TARGET_FILE:nes-codon-blas-plugin>"
        NES_CODON_OPENCV_PLUGIN_PATH="$<TARGET_FILE:nes-codon-opencv-plugin>"
        NES_CODON_COMPAT_PLUGIN_PATH="$<TARGET_FILE:nes-codon-compat-plugin>"
        NES_CODON_JSON_PLUGIN_PATH="$<TARGET_FILE:nes-codon-json-plugin>"
        NES_CODON_RE_PLUGIN_PATH="$<TARGET_FILE:nes-codon-re-plugin>"
        NES_CODON_UNICODE_PLUGIN_PATH="$<TARGET_FILE:nes-codon-unicode-plugin>"
        NES_HIPY_COMPILE_SCRIPT="${CMAKE_CURRENT_SOURCE_DIR}/../hipy/nes_hipy_compile.py"
        NES_NUMBA_COMPILE_SCRIPT="${CMAKE_CURRENT_SOURCE_DIR}/../numba/python/nes_numba_compile.py")

# Codon is built with the same RTTI mode as LLVM. Keep its filesystem adapter consistent because polymorphic
# shared_ptr objects cross this boundary.
if (NOT LLVM_ENABLE_RTTI)
    set_source_files_properties(PythonUdfFilesystem.cpp PROPERTIES COMPILE_OPTIONS "-fno-rtti")
endif ()

link_plugin_library(nes-udf nes-python-udf-compiler)
add_registry_entry(UdfCompiler Codon)
add_registry_entry(UdfCompiler Numba)
add_registry_entry(UdfCompiler HiPy)
