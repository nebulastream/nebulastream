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

#include <UdfCatalog.hpp>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>
#include <UdfCompilerRegistry.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

void UdfCatalog::registerUdf(
    std::string name,
    std::filesystem::path path,
    std::string entrypoint,
    std::vector<DataType> argTypes,
    DataType returnType,
    const UdfExecution execution,
    std::optional<InlineUdfSource> source,
    const UdfLanguage language,
    std::vector<uint64_t> bytesArguments,
    std::string compiler)
{
    if (source.has_value() && source->parameterNames.size() != argTypes.size())
    {
        throw NES::InvalidStatement(
            "UDF '{}' has {} parameter names for {} argument types", name, source->parameterNames.size(), argTypes.size());
    }
    /// The library path of an InProcess UDF is not checked here: the worker loads it, possibly on another machine, and
    /// reports a missing library when it loads the bridge.
    if (execution == UdfExecution::Inline)
    {
        if (!source.has_value() && !splitEntrypoint(entrypoint).has_value())
        {
            throw NES::InvalidStatement("Inline UDF '{}' needs ENTRYPOINT 'module.function', got '{}'", name, entrypoint);
        }
        if (argTypes.empty())
        {
            throw NES::InvalidStatement("Inline UDF '{}' needs at least one argument", name);
        }
    }

    /// Reject ABI-incompatible types up front so descriptor/signature compatibility is an invariant downstream code can trust.
    const auto validateType = [&](const DataType& type, const std::string_view role)
    {
        if (!isSupportedUdfType(type.type))
        {
            throw NES::CannotLoadUdf("UDF '{}' {} type is not supported: {}", name, role, type);
        }
    };
    for (const auto& argType : argTypes)
    {
        validateType(argType, "argument");
    }
    validateType(returnType, "return");
    std::ranges::sort(bytesArguments);
    for (const auto position : bytesArguments)
    {
        if (position >= argTypes.size()
            || (argTypes.at(position).type != DataType::Type::VARSIZED && argTypes.at(position).type != DataType::Type::CHAR))
        {
            throw NES::InvalidStatement("UDF '{}': only a VARSIZED or CHAR argument can be BYTES, not argument {}", name, position + 1);
        }
    }

    auto descriptor = UdfDescriptor{
        name,
        std::move(path),
        std::move(entrypoint),
        std::move(source),
        std::move(argTypes),
        std::move(bytesArguments),
        returnType,
        execution,
        language,
        std::nullopt,
        compiler};
    /// Compiled once here instead of for each call in a query. A build without Python UDFs has no compiler; the worker then
    /// reports that it cannot execute the UDF.
    if (execution == UdfExecution::Inline && language == UdfLanguage::Python)
    {
        if (const auto compile = UdfCompilerRegistry::instance().find(compiler))
        {
            descriptor.compiled = (*compile)(UdfCompilerRegistryArguments{.udf = descriptor, .importPaths = importPaths});
        }
        else if (compiler != "Codon")
        {
            throw NES::CannotLoadUdf("Inline UDF '{}': this build has no {} compiler", name, compiler);
        }
    }
    catalog.registerEntry(std::move(name), std::move(descriptor));
}

void UdfCatalog::removeUdf(const std::string& udfName)
{
    catalog.removeEntry(udfName);
}

bool UdfCatalog::hasUdf(const std::string& udfName) const
{
    return catalog.hasEntry(udfName);
}

std::vector<std::string> UdfCatalog::getUdfNames() const
{
    return catalog.getNames();
}

std::vector<UdfDescriptor> UdfCatalog::getRegisteredUdfs() const
{
    return catalog.getEntries();
}

UdfDescriptor UdfCatalog::load(const std::string& udfName) const
{
    return catalog.load(udfName, [&] { return NES::UnknownUdf("UDF '{}' was never registered", udfName); });
}

}
