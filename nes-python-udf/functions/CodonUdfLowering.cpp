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
/// A UDF named by its entry point, expressed as the inline function the Codon path compiles: the ENTRYPOINT module is
/// imported and called from a generated body. A nullable argument reaches the entry point as Optional[T], like any body.
InlineUdfSource buildCodonUdfCall(const UdfDescriptor& descriptor, const std::vector<DataType>& inputTypes)
{
    const auto entrypoint = splitEntrypoint(descriptor.getEntrypoint());
    INVARIANT(entrypoint.has_value(), "Codon UDF '{}' has no 'module.function' entry point", descriptor.getName());
    INVARIANT(
        inputTypes.size() == descriptor.getArgTypes().size(), "UDF '{}' called with a wrong number of arguments", descriptor.getName());

    InlineUdfSource call;
    std::vector<std::string> entryArguments;
    for (size_t index = 0; index < inputTypes.size(); ++index)
    {
        call.parameterNames.emplace_back(fmt::format("_p{}", index));
        /// Only the entry point's annotations can tell if a VARSIZED or CHAR value is meant as str or as bytes.
        const auto isText = inputTypes[index].type == DataType::Type::VARSIZED || inputTypes[index].type == DataType::Type::CHAR;
        entryArguments.emplace_back(isText ? fmt::format("_nes_argument(_nes_udf_entry, {0}, _p{0})", index) : fmt::format("_p{}", index));
    }
    call.body = fmt::format(
        "from {} import {} as _nes_udf_entry\nfrom nes_compat import _nes_argument, _nes_result\n"
        "return _nes_result(_nes_udf_entry({}))",
        entrypoint->first,
        entrypoint->second,
        fmt::join(entryArguments, ", "));
    return call;
}

/// Where Codon looks for the ENTRYPOINT module: the configured python_udf_import_paths plus NES_UDF_PATH, the module
/// search path the CPython bridge uses, so a single directory of UDF modules serves every backend.
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
    INVARIANT(arguments.outputType.nullable, "UDF call results are nullable");
    const auto importPaths = codonImportPaths(arguments.importPaths);
    const auto source
        = arguments.udf.getSource().has_value() ? *arguments.udf.getSource() : buildCodonUdfCall(arguments.udf, arguments.inputTypes);
    return PhysicalFunction{
        PythonPhysicalFunction(
            source.parameterNames, source.body, arguments.childFunctions, arguments.inputTypes, arguments.outputType, importPaths)}
        .withSetupChildren(std::move(arguments.childFunctions));
}

}
