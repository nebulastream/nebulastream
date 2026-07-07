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

#include <expected>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>

#include "Configurations/ConfigField.hpp"
#include "Configurations/ConfigValue.hpp"

namespace NES
{

/// Frontend-agnostic description of why a passed config does not satisfy a declared config schema.
struct InvalidConfigSpecification
{
    std::vector<QualifiedIdentifier> unresolvableFields;
    std::vector<std::pair<QualifiedIdentifier, Exception>> failedInstantiations;
    std::vector<QualifiedIdentifier> missingFields;

    friend std::ostream& operator<<(std::ostream& os, const InvalidConfigSpecification&);
};

/// Adapter for frontends that only have strings (worker YAML, systest files): typed
/// interpretation of a raw string config value, mirroring the SQL parser's literal typing —
/// bool, then signed integer (integers are always passed down as int64_t), then floating point;
/// falls back to the string. Call this at the frontend boundary; resolution itself only ever
/// sees ConfigLiterals.
[[nodiscard]] ConfigLiteral parseConfigLiteral(const std::string& raw);

/// Resolve passed (name, literal) pairs against a declared config schema:
/// each passed value is matched to a declared field (suffix lookup, so unqualified names work)
/// and instantiated via the field's factory; unmatched declared fields fall back to their default.
/// Each field sees exactly the literal the frontend produced — lowering it into the field's type
/// (e.g. uint64_t -> uint32_t) is the factory's job, see downcastConfigValue.
[[nodiscard]] std::expected<Schema<ConfigValue, Ordered>, InvalidConfigSpecification> resolveConfig(
    const std::vector<std::pair<QualifiedIdentifier, ConfigLiteral>>& passedConfig,
    const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig);

/// Convenience for unqualified-name maps: resolves and wraps the result.
[[nodiscard]] std::expected<InstantiatedConfig, InvalidConfigSpecification> instantiateConfig(
    const std::unordered_map<Identifier, ConfigLiteral>& passedConfig, const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig);

}

FMT_OSTREAM(NES::InvalidConfigSpecification);
