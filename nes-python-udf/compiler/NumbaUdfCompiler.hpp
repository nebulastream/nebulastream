/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <UdfCompilerRegistry.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

/// Compiles Inline Python UDFs declared with COMPILER NUMBA when the catalog registers them: runs
/// numba/python/nes_numba_compile.py with the Python of NES_NUMBA_PYTHON (an environment with Numba), which emits LLVM bitcode
/// with the ABI of the Codon entry, and finishes it like a Codon UDF (finishNumbaUdf).
struct NumbaUdfCompiler
{
    static CompiledInlineUdf createNumba(UdfCompilerRegistryArguments arguments);
};

}
