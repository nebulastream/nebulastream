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
#include <cstdint>
#include <functional>
#include <ostream>
#include <string>
#include <variant>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

/// Integer literals are always signed: frontends never produce an unsigned literal, and a field
/// that needs an unsigned type lowers the int64_t with a range check (see downcastConfigValue).
using ConfigLiteral = std::variant<std::monostate, std::string, int64_t, double, bool, Schema<UnqualifiedUnboundField, Ordered>>;
template <typename T>
concept isConfigLiteral = requires(T value) { ConfigLiteral{std::in_place_type<T>, std::move(value)}; };

/// A single (possibly qualified) config assignment as a frontend produced it, e.g.
/// `'ALL' AS "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES`
class LiteralConfigValue
{
    QualifiedIdentifier name;
    ConfigLiteral value;

public:
    LiteralConfigValue(QualifiedIdentifier name, ConfigLiteral value);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;

    [[nodiscard]] const ConfigLiteral& getValue() const;

    friend bool operator==(const LiteralConfigValue& lhs, const LiteralConfigValue& rhs);

    friend std::ostream& operator<<(std::ostream& os, const LiteralConfigValue& value);
};
}

template <>
struct std::hash<NES::LiteralConfigValue>
{
    size_t operator()(const NES::LiteralConfigValue& value) const noexcept;
};

FMT_OSTREAM(NES::LiteralConfigValue);
