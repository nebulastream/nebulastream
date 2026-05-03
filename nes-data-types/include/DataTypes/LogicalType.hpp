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
#include <DataTypes/Nullable.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Logical type identity at the schema and function-tree layer.
///
/// A LogicalType is purely an identity: a name plus optional named parameters
/// plus nullability. It carries no information about how the value is laid out
/// in physical memory — that mapping lives in LogicalTypeLoweringRegistry and
/// is resolved during the lowering phase.
///
/// Built-in primitive logical types (INTEGER, FLOAT, BOOL, TEXT) are registered
/// in nes-data-types' LogicalTypeRegistry; user-defined types (Point, Image,
/// ...) live in plugins and may either be scalar wrappers around a primitive
/// or compound types that flatten into multiple primitive fields at lowering.
class LogicalType final
{
public:
    struct Parameter
    {
        std::string name;
        std::string value;
        bool operator==(const Parameter&) const = default;
    };

    /// Default-constructed LogicalType is the sentinel "UNDEFINED" type — used
    /// when a function or schema field has not yet been type-inferred.
    LogicalType();
    LogicalType(std::string name, std::vector<Parameter> parameters, Nullable nullable);

    [[nodiscard]] const std::string& getName() const;
    [[nodiscard]] const std::vector<Parameter>& getParameters() const;
    [[nodiscard]] Nullable getNullable() const;
    [[nodiscard]] bool isNullable() const;

    /// Combine nullability of two logical types: result is nullable if either side is.
    [[nodiscard]] Nullable joinNullable(const LogicalType& other) const;

    /// Return a `LogicalType` that the given two types can be merged into.
    /// Pure logical-side rules (no physical bridge): identical names join to
    /// themselves; numeric promotion (`INTEGER` + `FLOAT` -> `FLOAT`); mismatch
    /// returns nullopt. Compound types only join with their identical name.
    /// Nullability is propagated via `joinNullable`.
    [[nodiscard]] std::optional<LogicalType> join(const LogicalType& other) const;

    /// Predicates for the four prototype primitive types. False for compound and
    /// other types (Point, ...). These are pure string-based queries — they do
    /// not bridge to physical `DataType`.
    [[nodiscard]] bool isInteger() const;
    [[nodiscard]] bool isFloat() const;
    [[nodiscard]] bool isNumeric() const;
    [[nodiscard]] bool isBool() const;
    [[nodiscard]] bool isText() const;
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
