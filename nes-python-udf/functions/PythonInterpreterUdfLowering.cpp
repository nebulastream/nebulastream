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

#include <PythonInterpreterUdfLowering.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Functions/UDFPhysicalFunction.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <UdfBackend.hpp>
#include <UdfBridgeRegistry.hpp>
#include <UdfLoweringRegistry.hpp>

namespace NES
{
namespace
{
/// Runs an inline PYTHON(...) body as a plain, top-level Python function handed to initialize_udf_from_source: the
/// CPython/PyPy ABI marshals arguments as ordinary Python values already, so no per-argument unwrapping is needed here
/// (unlike the Codon path, which must also generate its own C-ABI trampoline).
PhysicalFunction lowerInlineInterpreterUdf(const std::string_view bridge, UdfLoweringRegistryArguments arguments)
{
    const auto* source = std::get_if<InlineUdfSource>(&arguments.udf);
    INVARIANT(source != nullptr, "A catalog UDF with BRIDGE '{}' is an InProcess UDF", bridge);
    static constexpr std::string_view InlineFunctionName = "nes_inline_udf";
    auto pythonSource = fmt::format("def {}({}):\n{}\n", InlineFunctionName, fmt::join(source->parameterNames, ", "), source->body);
    auto backend = UdfBackend::createFromSource(
        resolveUdfBridgeLibraryPath(resolveUdfBridge(bridge)),
        pythonSource,
        std::string(InlineFunctionName),
        arguments.inputTypes,
        arguments.outputType);
    return PhysicalFunction{UDFPhysicalFunction(arguments.childFunctions, arguments.inputTypes, arguments.outputType, std::move(backend))}
        .withSetupChildren(std::move(arguments.childFunctions));
}
}

PhysicalFunction PythonInterpreterUdfLowering::createCPython(UdfLoweringRegistryArguments arguments)
{
    return lowerInlineInterpreterUdf("cpython", std::move(arguments));
}

PhysicalFunction PythonInterpreterUdfLowering::createPyPy(UdfLoweringRegistryArguments arguments)
{
    return lowerInlineInterpreterUdf("pypy", std::move(arguments));
}

}
