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

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>

namespace NES
{


/// Frontend-agnostic description of why a passed config does not satisfy a declared config schema.
struct InvalidConfigSpecification
{
    std::vector<QualifiedIdentifier> unresolvableFields{};
    std::vector<std::pair<QualifiedIdentifier, Exception>> failedInstantiations{};
    std::vector<QualifiedIdentifier> missingFields{};

    bool empty() const;

    static InvalidConfigSpecification combine(InvalidConfigSpecification lhs, InvalidConfigSpecification rhs);
    friend std::ostream& operator<<(std::ostream& os, const InvalidConfigSpecification&);
};

[[nodiscard]] Schema<LiteralConfigValue, Ordered> addDefaultConfigValues(const Schema<LiteralConfigValue, Ordered>& config, const Schema<ConfigFieldDefault, Ordered>& configDefaults);

[[nodiscard]] std::tuple<Schema<ConfigValue, Ordered>, InvalidConfigSpecification> applyConfigTransformations(Schema<ConfigValue, Ordered> config, const Schema<ConfigFieldTransformation, Unordered>& configTransformations);
/// Resolve the passed literal config values against a declared config schema:
/// each passed value is matched to a declared field (suffix lookup, so unqualified names work)
/// and instantiated via the field's factory; unmatched declared fields fall back to their default.
/// Each field sees exactly the literal the frontend produced — lowering it into the field's type
/// (e.g. int64_t -> uint32_t) is the factory's job, see downcastConfigValue.
[[nodiscard]] std::tuple<Schema<ConfigValue, Ordered>, InvalidConfigSpecification>
resolveConfig(const Schema<LiteralConfigValue, Ordered>& passedConfig, const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig);

std::expected<Schema<ConfigValue, Ordered>, InvalidConfigSpecification> toExpected(std::tuple<Schema<ConfigValue, Ordered>, InvalidConfigSpecification> result);
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
