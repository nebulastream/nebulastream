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

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <PythonPhysicalFunction.hpp>
#include <UdfDescriptor.hpp>
#include <UdfLoweringRegistry.hpp>

namespace NES
{
namespace
{
/// A catalog UDF that executes through Codon (CREATE FUNCTION ... BRIDGE 'codon'), expressed as the inline function
/// the Codon path already compiles: the ENTRYPOINT module is imported and called from a generated body.
/// Reproduces the strict-UDF semantics of the interpreter bridges (a NULL argument yields NULL without calling the UDF)
/// in the generated body, so the imported function only ever sees plain values, never Optional[T].
InlineUdfSource buildCodonUdfCall(const UdfDescriptor& descriptor, const std::vector<DataType>& inputTypes)
{
    const auto entrypoint = splitEntrypoint(descriptor.getEntrypoint());
    INVARIANT(entrypoint.has_value(), "Codon UDF '{}' has no 'module.function' entry point", descriptor.getName());
    INVARIANT(
        inputTypes.size() == descriptor.getArgTypes().size(), "UDF '{}' called with a wrong number of arguments", descriptor.getName());

    InlineUdfSource call;
    std::vector<std::string> arguments;
    call.body = fmt::format("from {} import {} as _nes_udf_entry\n", entrypoint->first, entrypoint->second);
    for (size_t index = 0; index < inputTypes.size(); ++index)
    {
        call.parameterNames.emplace_back(fmt::format("_p{}", index));
        if (inputTypes.at(index).nullable)
        {
            call.body += fmt::format("if _p{} is None:\n    return None\n", index);
            arguments.emplace_back(fmt::format("_p{}.__val__()", index));
        }
        else
        {
            arguments.emplace_back(fmt::format("_p{}", index));
        }
    }
    call.body += fmt::format("return _nes_udf_entry({})", fmt::join(arguments, ", "));
    return call;
}

/// Where Codon looks for the ENTRYPOINT module: the configured python_udf_import_paths plus NES_UDF_PATH, the module
/// search path the CPython and PyPy bridges use, so a single directory of UDF modules serves every backend.
std::vector<std::string> codonImportPaths(const std::vector<std::string>& configured)
{
    auto paths = configured;
    const auto* const udfPath = std::getenv("NES_UDF_PATH"); /// NOLINT(concurrency-mt-unsafe)
    if (udfPath == nullptr)
    {
        return paths;
    }
    const std::string_view remaining{udfPath};
    size_t begin = 0;
    while (begin <= remaining.size())
    {
        const auto end = remaining.find(':', begin);
        const auto directory = std::string{remaining.substr(begin, end == std::string_view::npos ? std::string_view::npos : end - begin)};
        if (!directory.empty() && std::ranges::find(paths, directory) == paths.end())
        {
            paths.push_back(directory);
        }
        if (end == std::string_view::npos)
        {
            break;
        }
        begin = end + 1;
    }
    return paths;
}
}

PhysicalFunction CodonUdfLowering::createCodon(UdfLoweringRegistryArguments arguments)
{
    auto importPaths = std::move(arguments.importPaths);
    InlineUdfSource source;
    if (const auto* descriptor = std::get_if<UdfDescriptor>(&arguments.udf))
    {
        INVARIANT(arguments.outputType.nullable, "UDF call results are nullable");
        source = buildCodonUdfCall(*descriptor, arguments.inputTypes);
        importPaths = codonImportPaths(importPaths);
    }
    else
    {
        source = std::get<InlineUdfSource>(std::move(arguments.udf));
    }
    return PhysicalFunction{
        PythonPhysicalFunction(
            source.parameterNames, source.body, arguments.childFunctions, arguments.inputTypes, arguments.outputType, importPaths)}
        .withSetupChildren(std::move(arguments.childFunctions));
}

}
