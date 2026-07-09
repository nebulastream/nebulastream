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

#include <filesystem>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

void UdfCatalog::registerUdf(
    std::string name, std::filesystem::path path, std::string entrypoint, std::vector<DataType> argTypes, DataType returnType)
{
    if (!std::filesystem::exists(path))
    {
        throw NES::InvalidStatement("UDF library path does not exist: {}", path);
    }
    if (!std::filesystem::is_regular_file(path))
    {
        throw NES::InvalidStatement("UDF library path is not a regular file: {}", path);
    }

    /// The backend maps each declared type to/from the UDF C ABI. Reject types
    /// the ABI cannot carry up front so `descriptor ↔ signature` compatibility is
    /// an invariant everything downstream (logical inference, lowering) can trust.
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

    auto descriptor = UdfDescriptor{name, std::move(path), std::move(entrypoint), std::move(argTypes), returnType};
    entries.insert_or_assign(std::move(name), std::move(descriptor));
}

void UdfCatalog::removeUdf(const std::string& udfName)
{
    entries.erase(udfName);
}

bool UdfCatalog::hasUdf(const std::string& udfName) const
{
    return entries.contains(udfName);
}

std::vector<std::string> UdfCatalog::getUdfNames() const
{
    std::vector<std::string> names;
    names.reserve(entries.size());
    for (const auto& [name, _] : entries)
    {
        names.push_back(name);
    }
    return names;
}

std::vector<UdfDescriptor> UdfCatalog::getRegisteredUdfs() const
{
    std::vector<UdfDescriptor> udfs;
    udfs.reserve(entries.size());
    for (const auto& [_, descriptor] : entries)
    {
        udfs.push_back(descriptor);
    }
    return udfs;
}

UdfDescriptor UdfCatalog::load(const std::string& udfName) const
{
    if (auto it = entries.find(udfName); it != entries.end())
    {
        return it->second;
    }
    throw NES::UnknownUdf("UDF '{}' was never registered", udfName);
}

}
