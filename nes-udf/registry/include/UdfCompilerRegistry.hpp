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

#include <functional>
#include <string>
#include <vector>
#include <Util/RuntimeRegistry.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

struct UdfCompilerRegistryArguments
{
    /// The UDF to compile, as the catalog registers it.
    UdfDescriptor udf;
    /// Where the compiler looks for the modules that the UDF imports, besides NES_UDF_PATH.
    std::vector<std::string> importPaths;
};

using UdfCompilerRegistryReturnType = CompiledInlineUdf;
using UdfCompilerFn = std::function<UdfCompilerRegistryReturnType(UdfCompilerRegistryArguments)>;

/// Compiles Inline UDFs when the catalog registers them, keyed by the compiler: Codon comes with nes-python-udf. The registry
/// is empty in a build without Python UDFs; the catalog then registers the UDF without compiling it.
class UdfCompilerRegistry : public RuntimeRegistry<UdfCompilerRegistry, std::string, UdfCompilerFn, /*CaseSensitive*/ false>
{
public:
    static UdfCompilerRegistry& instance();
};

}
