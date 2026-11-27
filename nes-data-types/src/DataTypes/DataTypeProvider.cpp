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

#include <DataTypes/DataTypeProvider.hpp>

#include <optional>
#include <string>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <DataTypes/ScalarTypes.hpp>
#include <magic_enum/magic_enum.hpp>
#include <DataTypeRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES::DataTypeProvider
{

std::optional<DataType> tryProvideDataType(const std::string& type)
{
    return tryProvideDataType(type, DataType::NULLABLE::NOT_NULLABLE);
}

std::optional<DataType> tryProvideDataType(const std::string& type, const DataType::NULLABLE isNullable)
{
    auto typeNameExp = QualifiedIdentifier::tryParse(type);
    if (!typeNameExp.has_value())
    {
        return std::nullopt;
    }
    const auto functorOpt = DataTypeRegistry::instance().find(typeNameExp.value());
    if (!functorOpt.has_value())
    {
        return std::nullopt;
    }
    auto nesTypeExp = functorOpt->instantiate(NESTypeParameters{});
    if (!nesTypeExp.has_value())
    {
        return std::nullopt;
    }
    auto finalType
        = (isNullable == DataType::NULLABLE::IS_NULLABLE) ? OptionalType::make(std::move(*nesTypeExp)) : std::move(*nesTypeExp);
    if (const auto* legacyTrait = finalType.tryGetTypeTrait<LegacyDataTypeTrait>())
    {
        return legacyTrait->getDataType();
    }
    return std::nullopt;
}

DataType provideDataType(const std::string& type)
{
    return provideDataType(type, DataType::NULLABLE::NOT_NULLABLE);
}

DataType provideDataType(const std::string& type, const DataType::NULLABLE isNullable)
{
    auto typeNameExp = QualifiedIdentifier::tryParse(type);
    if (!typeNameExp.has_value())
    {
        throw std::move(typeNameExp).error();
    }
    if (const auto resolved = tryProvideDataType(type, isNullable))
    {
        return *resolved;
    }
    throw UnknownPluginType("Unknown data type: {}", type);
}

DataType provideDataType(const DataType::Type type)
{
    return provideDataType(type, DataType::NULLABLE::NOT_NULLABLE);
}

DataType provideDataType(const DataType::Type type, const DataType::NULLABLE isNullable)
{
    const auto typeAsString = std::string(magic_enum::enum_name(type));
    return provideDataType(typeAsString, isNullable);
}

}
