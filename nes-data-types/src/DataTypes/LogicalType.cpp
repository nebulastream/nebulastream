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

#include <DataTypes/LogicalType.hpp>

#include <optional>
#include <ostream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Reflection.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>

namespace NES
{

LogicalType::LogicalType(std::string name, std::vector<Parameter> parameters, const DataType::NULLABLE nullable)
    : name(std::move(name)), parameters(std::move(parameters)), nullable(nullable == DataType::NULLABLE::IS_NULLABLE)
{
}

const std::string& LogicalType::getName() const
{
    return name;
}

const std::vector<LogicalType::Parameter>& LogicalType::getParameters() const
{
    return parameters;
}

DataType::NULLABLE LogicalType::getNullable() const
{
    return nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
}

bool LogicalType::isNullable() const
{
    return nullable;
}

std::optional<DataType> LogicalType::toPhysical() const
{
    if (const auto type = magic_enum::enum_cast<DataType::Type>(name); type.has_value())
    {
        return DataTypeProvider::provideDataType(
            type.value(), nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    }
    return std::nullopt;
}

LogicalType LogicalType::fromPhysical(const DataType& dataType)
{
    return LogicalType{
        std::string{magic_enum::enum_name(dataType.type)},
        {},
        dataType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE};
}

std::optional<LogicalType> LogicalType::join(const LogicalType& other) const
{
    const auto leftPhysical = toPhysical();
    const auto rightPhysical = other.toPhysical();
    if (not leftPhysical.has_value() or not rightPhysical.has_value())
    {
        return std::nullopt;
    }
    if (const auto joined = leftPhysical->join(rightPhysical.value()); joined.has_value())
    {
        return LogicalType::fromPhysical(joined.value());
    }
    return std::nullopt;
}

DataType::NULLABLE LogicalType::joinNullable(const LogicalType& other) const
{
    return (this->nullable or other.nullable) ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
}

bool LogicalType::isInteger() const
{
    if (const auto physical = toPhysical(); physical.has_value())
    {
        return physical->isInteger();
    }
    return false;
}

bool LogicalType::isSignedInteger() const
{
    if (const auto physical = toPhysical(); physical.has_value())
    {
        return physical->isSignedInteger();
    }
    return false;
}

bool LogicalType::isFloat() const
{
    if (const auto physical = toPhysical(); physical.has_value())
    {
        return physical->isFloat();
    }
    return false;
}

bool LogicalType::isNumeric() const
{
    return isInteger() or isFloat();
}

bool LogicalType::isUndefined() const
{
    if (const auto physical = toPhysical(); physical.has_value())
    {
        return physical->type == DataType::Type::UNDEFINED;
    }
    return false;
}

std::ostream& operator<<(std::ostream& os, const LogicalType& logicalType)
{
    if (logicalType.parameters.empty())
    {
        return os << fmt::format("LogicalType({} nullable: {})", logicalType.name, logicalType.nullable);
    }
    std::vector<std::string> formattedParameters;
    formattedParameters.reserve(logicalType.parameters.size());
    for (const auto& parameter : logicalType.parameters)
    {
        formattedParameters.push_back(fmt::format("{}={}", parameter.name, parameter.value));
    }
    return os << fmt::format("LogicalType({}({}) nullable: {})", logicalType.name, fmt::join(formattedParameters, ", "), logicalType.nullable);
}

Reflected Reflector<LogicalType::Parameter>::operator()(const LogicalType::Parameter& parameter) const
{
    return reflect(std::make_pair(parameter.name, parameter.value));
}

LogicalType::Parameter Unreflector<LogicalType::Parameter>::operator()(const Reflected& rfl) const
{
    auto [name, value] = unreflect<std::pair<std::string, std::string>>(rfl);
    return LogicalType::Parameter{std::move(name), std::move(value)};
}

Reflected Reflector<LogicalType>::operator()(const LogicalType& logicalType) const
{
    return reflect(std::make_tuple(logicalType.getName(), logicalType.getParameters(), logicalType.isNullable()));
}

LogicalType Unreflector<LogicalType>::operator()(const Reflected& rfl) const
{
    auto [name, parameters, nullable] = unreflect<std::tuple<std::string, std::vector<LogicalType::Parameter>, bool>>(rfl);
    return LogicalType{
        std::move(name), std::move(parameters), nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE};
}

}
