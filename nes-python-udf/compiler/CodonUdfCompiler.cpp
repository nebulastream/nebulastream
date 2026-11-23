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

#include <CodonUdfCompiler.hpp>

#include <atomic>
#include <cstdint>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <fmt/format.h>
#include <PythonUdfCompiler.hpp>
#include <UdfCompilerRegistry.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

CompiledInlineUdf CodonUdfCompiler::createCodon(UdfCompilerRegistryArguments arguments)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
    static std::atomic_uint64_t nextRegisteredUdfId = 0;
    const auto& udf = arguments.udf;
    /// One compile serves every call, so the entry point takes every argument as nullable and returns a nullable result. The
    /// body receives the arguments as declared: Optional only if declared nullable; NULL for a NOT NULL parameter gives NULL.
    auto argumentTypes = udf.getArgTypes();
    std::vector<bool> bodyNullable;
    for (auto& type : argumentTypes)
    {
        bodyNullable.push_back(type.nullable);
        type.nullable = true;
    }
    auto returnType = udf.getReturnType();
    returnType.nullable = true;
    const auto source = udf.getSource().has_value() ? *udf.getSource() : codonUdfSource(udf, argumentTypes);
    return compilePythonUdf(
        fmt::format("__nes_python_udf_registered_{}", nextRegisteredUdfId.fetch_add(1)),
        source.parameterNames,
        source.body,
        argumentTypes,
        returnType,
        codonImportPaths(arguments.importPaths),
        udf.getBytesArguments(),
        bodyNullable,
        udf.getPythonCompat());
}

}
