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
#include <expected>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Hash.hpp>
#include <Util/Logger/Formatter.hpp>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

#include "Configurations/ConfigField.hpp"
#include "Configurations/ConfigValue.hpp"

namespace NES
{

/// A single (possibly qualified) config assignment as a frontend produced it, e.g.
/// `'ALL' AS "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES`. The literal is typed by the
/// frontend (integers are always signed, see ConfigLiteral).
class LiteralConfigValue
{
    QualifiedIdentifier name;
    ConfigLiteral value;

public:
    LiteralConfigValue(QualifiedIdentifier name, ConfigLiteral value) : name(std::move(name)), value(std::move(value)) { }

    LiteralConfigValue(std::string name, ConfigLiteral value)
        : name(QualifiedIdentifier::create(Identifier::parse(std::move(name)))), value(std::move(value))
    {
    }

    [[nodiscard]] QualifiedIdentifier getFullyQualifiedName() const { return name; }
    [[nodiscard]] ConfigLiteral getValue() const { return value; }

    friend bool operator==(const LiteralConfigValue& lhs, const LiteralConfigValue& rhs) = default;

    friend std::ostream& operator<<(std::ostream& os, const LiteralConfigValue& value) { return os << value.name; }
};

/// Frontend-agnostic description of why a passed config does not satisfy a declared config schema.
struct InvalidConfigSpecification
{
    std::vector<QualifiedIdentifier> unresolvableFields;
    std::vector<std::pair<QualifiedIdentifier, Exception>> failedInstantiations;
    std::vector<QualifiedIdentifier> missingFields;

    friend std::ostream& operator<<(std::ostream& os, const InvalidConfigSpecification&);
};

/// Resolve the passed literal config values against a declared config schema:
/// each passed value is matched to a declared field (suffix lookup, so unqualified names work)
/// and instantiated via the field's factory; unmatched declared fields fall back to their default.
/// Each field sees exactly the literal the frontend produced — lowering it into the field's type
/// (e.g. int64_t -> uint32_t) is the factory's job, see downcastConfigValue.
[[nodiscard]] std::expected<Schema<ConfigValue, Ordered>, InvalidConfigSpecification>
resolveConfig(const Schema<LiteralConfigValue, Ordered>& passedConfig, const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig);

}

template <>
struct std::hash<NES::LiteralConfigValue>
{
    size_t operator()(const NES::LiteralConfigValue& value) const noexcept
    {
        return folly::hash::hash_combine_generic(NES::Hash{}, value.getFullyQualifiedName(), value.getValue());
    }
};

FMT_OSTREAM(NES::LiteralConfigValue);
FMT_OSTREAM(NES::InvalidConfigSpecification);
