# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Runs an INLINE Python UDF without the query engine (UdfRunner.cpp). A benchmark tool: no test uses it.
find_package(MLIR REQUIRED CONFIG)
add_executable(nes-python-udf-runner UdfRunner.cpp)
target_link_libraries(nes-python-udf-runner PRIVATE
        nes-udf nes-data-types nes-python-udf nes-python-udf-compiler nes-numba-runtime nes-codon-plugin
        LLVMOrcJIT LLVMPasses LLVMBitReader LLVM${LLVM_NATIVE_ARCH}CodeGen LLVM${LLVM_NATIVE_ARCH}AsmParser LLVM${LLVM_NATIVE_ARCH}Desc
        LLVM${LLVM_NATIVE_ARCH}Info)
target_include_directories(nes-python-udf-runner SYSTEM PRIVATE ${LLVM_INCLUDE_DIRS})
# The Codon plugins resolve seq_alloc and the other runtime functions from the executable, as from systest and the worker.
set_property(TARGET nes-python-udf-runner PROPERTY ENABLE_EXPORTS ON)
