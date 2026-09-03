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
#include <tuple>
#include <utility>
#include <vector>

#include <Configurations/ConfigLiteral.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>

namespace NES
{


/// Frontend-agnostic description of why a passed config does not satisfy a declared config schema.
struct ConfigResolutionErrors
{
    struct UnresolvableField
    {
        QualifiedIdentifier targetName;
        std::vector<QualifiedIdentifier> conflictsWith;
        friend std::ostream& operator<<(std::ostream& os, const UnresolvableField& field);
        bool operator==(const UnresolvableField&) const = default;
    };

    std::vector<UnresolvableField> unresolvableFields;
    std::vector<std::pair<QualifiedIdentifier, Exception>> failedInstantiations;
    std::vector<QualifiedIdentifier> missingFields;

    [[nodiscard]] bool empty() const;

    /// Merges two instances of this class together, usually used to produce a single error message for the errors of resolveConfig and applyConfigTransformations
    static ConfigResolutionErrors combine(ConfigResolutionErrors lhs, ConfigResolutionErrors rhs);
    friend std::ostream& operator<<(std::ostream& os, const ConfigResolutionErrors&);
};

[[nodiscard]] std::tuple<Schema<InstantiatedConfigValue, Ordered>, ConfigResolutionErrors> applyConfigTransformations(
    Schema<InstantiatedConfigValue, Ordered> config, const Schema<ConfigFieldTransformation, Unordered>& configTransformations);
/// Resolve the passed literal config values against a declared config schema:
/// each passed value is matched to a declared field (suffix lookup, so unqualified names work)
/// and instantiated via the field's factory; unmatched declared fields fall back first to the explicit configDefaults
/// and then to the default that is part of the field.
/// Each field sees exactly the literal the frontend produced — lowering it into the field's type
/// (e.g. int64_t -> uint32_t) is the factory's job, see downcastConfigValue.
/// @returns a tuple of the resolved config and any errors encountered. Allows a frontend to either discard certain errors,
/// or combine them with errors from another source before aborting and reporting them to the user.
/// When the validation for a passed literal value fails, and the config field has a declared default,
/// it will fall back to the default and report record it as a failed instantiation.
[[nodiscard]] std::tuple<Schema<InstantiatedConfigValue, Ordered>, ConfigResolutionErrors> resolveConfig(
    const Schema<LiteralConfigValue, Ordered>& passedConfig,
    const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig,
    const Schema<ConfigFieldDefault, Ordered>& configDefaults = {}); /// NOLINT(fuchsia-default-arguments-declarations)

[[nodiscard]] std::expected<Schema<InstantiatedConfigValue, Ordered>, ConfigResolutionErrors>
toExpected(std::tuple<Schema<InstantiatedConfigValue, Ordered>, ConfigResolutionErrors> result);

[[nodiscard]] std::tuple<Schema<InstantiatedConfigValue, Ordered>, ConfigResolutionErrors> resolveConfigFullyQualified(
    const Schema<LiteralConfigValue, Ordered>& passedConfig,
    const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig,
    const Schema<ConfigFieldDefault, Ordered>& configDefaults = {}); /// NOLINT(fuchsia-default-arguments-declarations)

}

FMT_OSTREAM(NES::ConfigResolutionErrors::UnresolvableField);
FMT_OSTREAM(NES::ConfigResolutionErrors);
