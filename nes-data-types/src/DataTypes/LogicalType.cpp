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

#include <ostream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include <DataTypes/Nullable.hpp>
#include <Util/Reflection.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

namespace NES
{

LogicalType::LogicalType() : name("UNDEFINED"), nullable(true)
{
}

LogicalType::LogicalType(std::string name, std::vector<Parameter> parameters, const Nullable nullable)
    : name(std::move(name)), parameters(std::move(parameters)), nullable(nullable == Nullable::IS_NULLABLE)
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

Nullable LogicalType::getNullable() const
{
    return nullable ? Nullable::IS_NULLABLE : Nullable::NOT_NULLABLE;
}

bool LogicalType::isNullable() const
{
    return nullable;
}

Nullable LogicalType::joinNullable(const LogicalType& other) const
{
    return (this->nullable or other.nullable) ? Nullable::IS_NULLABLE : Nullable::NOT_NULLABLE;
}

bool LogicalType::isInteger() const
{
    return name == "INTEGER";
}

bool LogicalType::isFloat() const
{
    return name == "FLOAT";
}

bool LogicalType::isNumeric() const
{
    return isInteger() or isFloat();
}

bool LogicalType::isBool() const
{
    return name == "BOOL";
}

bool LogicalType::isText() const
{
    return name == "TEXT";
}

bool LogicalType::isUndefined() const
{
    return name == "UNDEFINED";
}

size_t LogicalType::byteSize() const
{
    /// Mirrors `DataType::getSizeInBytesWithNull` for the primitive lowerings
    /// performed by `nes-physical-types`'s `toPhysical` bridge. Update both in
    /// lockstep — the lowering layer assumes the prototype names INTEGER /
    /// FLOAT / BOOL / TEXT are equivalent to INT64 / FLOAT64 / BOOLEAN /
    /// VARSIZED at the physical level.
    if (name == "INTEGER" or name == "INT64" or name == "UINT64" or name == "FLOAT" or name == "FLOAT64")
    {
        return 8;
    }
    if (name == "INT32" or name == "UINT32" or name == "FLOAT32")
    {
        return 4;
    }
    if (name == "INT16" or name == "UINT16")
    {
        return 2;
    }
    if (name == "INT8" or name == "UINT8" or name == "BOOL" or name == "BOOLEAN" or name == "CHAR")
    {
        return 1;
    }
    if (name == "TEXT" or name == "VARSIZED")
    {
        return 16;
    }
    return 0;
}

std::optional<LogicalType> LogicalType::join(const LogicalType& other) const
{
    const auto resultNullable = joinNullable(other);
    if (name == other.name)
    {
        return LogicalType{name, parameters, resultNullable};
    }
    /// Numeric promotion: any combination of INTEGER and FLOAT widens to FLOAT.
    if ((isInteger() and other.isFloat()) or (isFloat() and other.isInteger()))
    {
        return LogicalType{"FLOAT", {}, resultNullable};
    }
    return std::nullopt;
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
        std::move(name), std::move(parameters), nullable ? Nullable::IS_NULLABLE : Nullable::NOT_NULLABLE};
}

}
