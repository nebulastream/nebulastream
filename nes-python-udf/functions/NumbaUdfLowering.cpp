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

#include <NumbaUdfLowering.hpp>

#include <utility>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <ErrorHandling.hpp>
#include <PythonPhysicalFunction.hpp>
#include <UdfDescriptor.hpp>
#include <UdfLoweringRegistry.hpp>

namespace NES
{
PhysicalFunction NumbaUdfLowering::createNumba(UdfLoweringRegistryArguments arguments)
{
    INVARIANT(arguments.outputType.nullable, "UDF call results are nullable");
    const auto& udf = arguments.udf;
    const auto& compiled = udf.getCompiled();
    if (!compiled.has_value())
    {
        throw QueryCompilerError("Numba UDF '{}' was not compiled when it was registered", udf.getName());
    }
    /// As for Codon: the entry point takes nullable arguments (NumbaUdfCompiler).
    auto argumentTypes = udf.getArgTypes();
    for (auto& type : argumentTypes)
    {
        type.nullable = true;
    }
    return PhysicalFunction{
        PythonPhysicalFunction(
            *compiled, arguments.childFunctions, std::move(argumentTypes), arguments.outputType, InlineUdfRuntime::Numba)}
        .withSetupChildren(std::move(arguments.childFunctions));
}
}
