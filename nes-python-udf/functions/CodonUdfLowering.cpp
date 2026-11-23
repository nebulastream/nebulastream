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

#include <CodonUdfLowering.hpp>

#include <atomic>
#include <ranges>
#include <cstddef>
#include <cstdlib>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <PythonPhysicalFunction.hpp>
#include <PythonUdfCompiler.hpp>
#include <UdfDescriptor.hpp>
#include <UdfLoweringRegistry.hpp>

namespace NES
{
PhysicalFunction CodonUdfLowering::createCodon(UdfLoweringRegistryArguments arguments)
{
    INVARIANT(arguments.outputType.nullable, "UDF call results are nullable");
    const auto& udf = arguments.udf;
    /// A UDF from the catalog was compiled when it was registered (UdfCatalog::registerUdf); its entry point takes nullable
    /// arguments and returns a nullable result (CodonUdfCompiler). Others, e.g. defined in code, are compiled here for the
    /// types of this call.
    if (const auto& compiled = udf.getCompiled())
    {
        auto argumentTypes = udf.getArgTypes();
        for (auto& type : argumentTypes)
        {
            type.nullable = true;
        }
        return PhysicalFunction{PythonPhysicalFunction(*compiled, arguments.childFunctions, std::move(argumentTypes), arguments.outputType)}
            .withSetupChildren(std::move(arguments.childFunctions));
    }
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
    static std::atomic_uint64_t nextLoweredUdfId = 0;
    const auto source = udf.getSource().has_value() ? *udf.getSource() : codonUdfSource(udf, arguments.inputTypes);
    const auto compiled = compilePythonUdf(
        fmt::format("__nes_python_udf_lowered_{}", nextLoweredUdfId.fetch_add(1)),
        source.parameterNames,
        source.body,
        arguments.inputTypes,
        arguments.outputType,
        codonImportPaths(arguments.importPaths),
        udf.getBytesArguments(),
        arguments.inputTypes | std::views::transform([](const DataType& type) { return type.nullable; }) | std::ranges::to<std::vector>(),
        udf.getPythonCompat());
    return PhysicalFunction{PythonPhysicalFunction(compiled, arguments.childFunctions, arguments.inputTypes, arguments.outputType)}
        .withSetupChildren(std::move(arguments.childFunctions));
}

}
