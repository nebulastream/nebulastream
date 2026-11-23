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

/// Compiles Inline Python UDFs declared with COMPILER HIPY when the catalog registers them: runs hipy/nes_hipy_compile.py with
/// the Python of NES_HIPY_PYTHON (default python3) and the HiPy checkout of NES_HIPY_DIR. HiPy traces the UDF with abstract
/// arguments of the declared types into its IR. The result holds that IR as JSON in `pipelineBitcode`, and
/// HiPyPhysicalFunction traces it with Nautilus when a query lowers the UDF call.
struct HiPyUdfCompiler
{
    static CompiledInlineUdf createHiPy(UdfCompilerRegistryArguments arguments);
};

}
