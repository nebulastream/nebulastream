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

#include <cstddef>
#include <functional>
#include <optional>
#include <ostream>
#include <string>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Logical type identity at the schema and function-tree layer.
///
/// A LogicalType is purely an identity: a name plus optional named parameters
/// plus nullability. It carries no information about how the value is laid out
/// in physical memory — that mapping lives in LogicalTypeLoweringRegistry and
/// is resolved during the new logical-to-primitive lowering phase.
///
/// Built-in logical types (INT32, FLOAT64, ...) share the name of the
/// underlying physical DataType; user-defined types (Point, Image, ...) live
/// in plugins and may either be scalar wrappers around a physical type or
/// compound types that flatten into multiple primitive fields at lowering time.
class LogicalType final
{
public:
    struct Parameter
    {
        std::string name;
        std::string value;
        bool operator==(const Parameter&) const = default;
    };

    LogicalType(std::string name, std::vector<Parameter> parameters, DataType::NULLABLE nullable);

    [[nodiscard]] const std::string& getName() const;
    [[nodiscard]] const std::vector<Parameter>& getParameters() const;
    [[nodiscard]] DataType::NULLABLE getNullable() const;
    [[nodiscard]] bool isNullable() const;

    /// Prototype-level bridge to the existing physical-type machinery: every built-in
    /// primitive logical type's name matches a DataType::Type enum name (INT32, ...),
    /// so the underlying physical type can be recovered without a registry lookup.
    /// Returns nullopt for plugin-defined / compound logical types.
    [[nodiscard]] std::optional<DataType> toPhysical() const;

    /// Wrap a primitive physical type as the matching primitive logical type.
    static LogicalType fromPhysical(const DataType& dataType);

    /// Mirrors DataType::join for primitive logical types — resolves both sides to
    /// physical types, joins them, and re-wraps. Returns nullopt if either side is
    /// non-primitive (compound) or if joining is undefined.
    [[nodiscard]] std::optional<LogicalType> join(const LogicalType& other) const;
    [[nodiscard]] DataType::NULLABLE joinNullable(const LogicalType& other) const;

    /// Predicates inherited from primitive DataType semantics. Always false for
    /// non-primitive logical types.
    [[nodiscard]] bool isInteger() const;
    [[nodiscard]] bool isSignedInteger() const;
    [[nodiscard]] bool isFloat() const;
    [[nodiscard]] bool isNumeric() const;
    [[nodiscard]] bool isUndefined() const;

    bool operator==(const LogicalType& other) const = default;

    friend std::ostream& operator<<(std::ostream& os, const LogicalType& logicalType);

private:
    std::string name;
    std::vector<Parameter> parameters;
    bool nullable;
};

template <>
struct Reflector<LogicalType::Parameter>
{
    Reflected operator()(const LogicalType::Parameter& parameter) const;
};

template <>
struct Unreflector<LogicalType::Parameter>
{
    LogicalType::Parameter operator()(const Reflected& rfl) const;
};

template <>
struct Reflector<LogicalType>
{
    Reflected operator()(const LogicalType& logicalType) const;
};

template <>
struct Unreflector<LogicalType>
{
    LogicalType operator()(const Reflected& rfl) const;
};

}

template <>
struct std::hash<NES::LogicalType::Parameter>
{
    size_t operator()(const NES::LogicalType::Parameter& parameter) const noexcept
    {
        const size_t nameHash = std::hash<std::string>{}(parameter.name);
        const size_t valueHash = std::hash<std::string>{}(parameter.value);
        return nameHash ^ (valueHash + 0x9e3779b9 + (nameHash << 6) + (nameHash >> 2));
    }
};

template <>
struct std::hash<NES::LogicalType>
{
    size_t operator()(const NES::LogicalType& logicalType) const noexcept
    {
        size_t result = std::hash<std::string>{}(logicalType.getName());
        for (const auto& parameter : logicalType.getParameters())
        {
            const size_t parameterHash = std::hash<NES::LogicalType::Parameter>{}(parameter);
            result ^= parameterHash + 0x9e3779b9 + (result << 6) + (result >> 2);
        }
        result ^= static_cast<size_t>(logicalType.isNullable());
        return result;
    }
};

FMT_OSTREAM(NES::LogicalType);
