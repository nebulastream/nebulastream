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

#include <Configurations/ConfigResolution.hpp>

#include <algorithm>
#include <cstdint>
#include <expected>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

std::ostream& operator<<(std::ostream& os, const InvalidConfigSpecification& errors)
{
    os << "Invalid config specification:";
    if (not errors.unresolvableFields.empty())
    {
        os << fmt::format("\nUnresolvable fields: {}", fmt::join(errors.unresolvableFields, ", "));
    }
    if (not errors.failedInstantiations.empty())
    {
        os << fmt::format(
            "\nFailed instantiations: {}",
            fmt::join(
                errors.failedInstantiations
                    | std::views::transform([](const auto& failed) { return fmt::format("{}: {}", failed.first, failed.second.what()); }),
                ", "));
    }
    if (not errors.missingFields.empty())
    {
        os << fmt::format("\nMissing fields: {}", fmt::join(errors.missingFields, ", "));
    }
    return os;
}

ConfigLiteral parseConfigLiteral(const std::string& raw)
{
    const auto lowered = toLowerCase(raw);
    if (lowered == "true")
    {
        return true;
    }
    if (lowered == "false")
    {
        return false;
    }
    /// Integer literals are always passed down signed (int64_t), matching the SQL parser; a
    /// field's factory lowers into its declared type via downcastConfigValue.
    if (const auto asSigned = from_chars<int64_t>(raw))
    {
        return *asSigned;
    }
    if (const auto asDouble = from_chars<double>(raw))
    {
        return *asDouble;
    }
    return raw;
}

std::expected<Schema<ConfigValue, Ordered>, InvalidConfigSpecification> resolveConfig(
    const std::vector<std::pair<QualifiedIdentifier, ConfigLiteral>>& passedConfig,
    const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig)
{
    InvalidConfigSpecification errors;
    std::vector<ConfigValue> resolvedConfig;

    for (const auto& [passedName, passedLiteral] : passedConfig)
    {
        const auto configField = declaredConfig.getFieldByName(passedName);
        if (not configField.has_value())
        {
            errors.unresolvableFields.push_back(passedName);
            continue;
        }
        auto created = configField->apply(passedLiteral);
        if (not created.has_value())
        {
            errors.failedInstantiations.emplace_back(passedName, created.error());
            continue;
        }
        resolvedConfig.emplace_back(configField->getFullyQualifiedName(), std::move(created).value().getValue());
    }

    /// I don't want to use an unordered_map<QualifiedIdentifier, ConfigValue> for resolved config just to avoid this n x m comparison
    /// to keep the order of the values in resolved Config. If we'd add positional arguments, we just have to ensure that optional values
    /// must come after any passed argument in the order of the declared config, to avoid unintentionally passing a value as the wrong
    /// parameter because something was filled in by a default value.
    for (const auto& declaredField : declaredConfig)
    {
        const auto alreadyResolved = std::ranges::any_of(
            resolvedConfig,
            [&declaredField](const ConfigValue& exists) { return exists.getFullyQualifiedName() == declaredField.getFullyQualifiedName(); });
        if (alreadyResolved)
        {
            continue;
        }
        if (declaredField.hasDefault())
        {
            resolvedConfig.emplace_back(declaredField.getFullyQualifiedName(), declaredField.getDefault());
        }
        else
        {
            errors.missingFields.push_back(declaredField.getFullyQualifiedName());
        }
    }

    if (not errors.unresolvableFields.empty() or not errors.failedInstantiations.empty() or not errors.missingFields.empty())
    {
        return std::unexpected{std::move(errors)};
    }
    return Schema<ConfigValue, Ordered>{std::move(resolvedConfig)};
}

std::expected<InstantiatedConfig, InvalidConfigSpecification> instantiateConfig(
    const std::unordered_map<Identifier, ConfigLiteral>& passedConfig, const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig)
{
    std::vector<std::pair<QualifiedIdentifier, ConfigLiteral>> passedLiterals;
    passedLiterals.reserve(passedConfig.size());
    for (const auto& [name, value] : passedConfig)
    {
        passedLiterals.emplace_back(QualifiedIdentifier{std::vector{name}}, value);
    }
    return resolveConfig(passedLiterals, declaredConfig).transform([](auto&& resolved) { return InstantiatedConfig{std::move(resolved)}; });
}

}
