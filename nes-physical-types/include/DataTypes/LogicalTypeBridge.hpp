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

#pragma once

#include <optional>
#include <string>
#include <DataTypes/DataType.hpp>
#include <DataTypes/LogicalType.hpp>
#include <DataTypes/Nullable.hpp>
#include <magic_enum/magic_enum.hpp>

namespace NES
{

/// Wrap a primitive physical DataType as the matching primitive LogicalType.
/// The resulting LogicalType's name is the magic-enum name of `DataType::Type`
/// (so `INT64`, `FLOAT64`, `BOOLEAN`, ...). Used as a bridge by older
/// LogicalFunction classes that still carry a DataType stamp internally.
inline LogicalType fromPhysical(const DataType& dataType)
{
    return LogicalType{
        std::string{magic_enum::enum_name(dataType.type)},
        {},
        dataType.nullable ? Nullable::IS_NULLABLE : Nullable::NOT_NULLABLE};
}

/// Recover a physical DataType from a primitive LogicalType. Built-in primitive
/// logical types share their name with a DataType::Type enum value; non-primitive
/// (compound) logical types have no DataType counterpart and return nullopt.
///
/// Also handles the prototype's reduced LogicalType vocabulary, which collapses
/// the full enum to four user-facing names: INTEGER -> INT64, FLOAT -> FLOAT64,
/// BOOL -> BOOLEAN, TEXT -> VARSIZED.
inline std::optional<DataType> toPhysical(const LogicalType& logicalType)
{
    if (const auto type = magic_enum::enum_cast<DataType::Type>(logicalType.getName()); type.has_value())
    {
        return DataType{type.value(), logicalType.getNullable()};
    }
    const auto& name = logicalType.getName();
    if (name == "INTEGER")
    {
        return DataType{DataType::Type::INT64, logicalType.getNullable()};
    }
    if (name == "FLOAT")
    {
        return DataType{DataType::Type::FLOAT64, logicalType.getNullable()};
    }
    if (name == "BOOL")
    {
        return DataType{DataType::Type::BOOLEAN, logicalType.getNullable()};
    }
    if (name == "TEXT")
    {
        return DataType{DataType::Type::VARSIZED, logicalType.getNullable()};
    }
    return std::nullopt;
}

}
