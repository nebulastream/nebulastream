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
#include <expected>
#include <ranges>
#include <utility>
#include <vector>

#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
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

std::expected<Schema<ConfigValue, Ordered>, InvalidConfigSpecification>
resolveConfig(const Schema<LiteralConfigValue, Ordered>& passedConfig, const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig)
{
    InvalidConfigSpecification errors;
    std::vector<ConfigValue> resolvedConfig;

    for (const auto& passedValue : passedConfig)
    {
        const auto configField = declaredConfig.getFieldByName(passedValue.getFullyQualifiedName());
        if (not configField.has_value())
        {
            errors.unresolvableFields.push_back(passedValue.getFullyQualifiedName());
            continue;
        }
        auto created = configField->apply(passedValue.getValue());
        if (not created.has_value())
        {
            errors.failedInstantiations.emplace_back(passedValue.getFullyQualifiedName(), created.error());
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

}
