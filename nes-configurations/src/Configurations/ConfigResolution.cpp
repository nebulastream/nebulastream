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
#include <array>
#include <expected>
#include <functional>
#include <ostream>
#include <ranges>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

std::ostream& operator<<(std::ostream& os, const ConfigResolutionErrors::UnresolvableField& field)
{
    if (field.conflictsWith.empty())
    {
        return os << fmt::format("{}", field.targetName);
    }
    return os << fmt::format("{} is ambiguous and could resolve to ({})", field.targetName, fmt::join(field.conflictsWith, ", and "));
}

std::ostream& operator<<(std::ostream& os, const ConfigResolutionErrors& errors)
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

bool ConfigResolutionErrors::empty() const
{
    return unresolvableFields.empty() && failedInstantiations.empty() && missingFields.empty();
}

ConfigResolutionErrors ConfigResolutionErrors::combine(ConfigResolutionErrors lhs, ConfigResolutionErrors rhs)
{
    return ConfigResolutionErrors{
        .unresolvableFields = std::array{std::move(lhs.unresolvableFields), std::move(rhs.unresolvableFields)} | std::views::join
            | std::ranges::to<std::vector>(),
        .failedInstantiations = std::array{std::move(lhs.failedInstantiations), std::move(rhs.failedInstantiations)} | std::views::join
            | std::ranges::to<std::vector>(),
        .missingFields
        = std::array{std::move(lhs.missingFields), std::move(rhs.missingFields)} | std::views::join | std::ranges::to<std::vector>()};
}

namespace
{
std::tuple<Schema<InstantiatedConfigValue, Ordered>, ConfigResolutionErrors> resolveConfig(
    const Schema<LiteralConfigValue, Ordered>& passedConfig,
    const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig,
    const Schema<ConfigFieldDefault, Ordered>& configDefaults,
    const std::function<std::expected<QualifiedErasedConfigField, std::vector<QualifiedIdentifier>>(const QualifiedIdentifier&)>& lookupFn)
{
    ConfigResolutionErrors errors;
    std::vector<InstantiatedConfigValue> resolvedConfig;

    for (const auto& passedValue : passedConfig)
    {
        const auto configField = lookupFn(passedValue.getFullyQualifiedName());
        if (not configField.has_value())
        {
            errors.unresolvableFields.emplace_back(passedValue.getFullyQualifiedName(), configField.error());
            continue;
        }
        auto created = configField->apply(passedValue.getValue());
        if (not created.has_value())
        {
            errors.failedInstantiations.emplace_back(passedValue.getFullyQualifiedName(), created.error());
            continue;
        }
        resolvedConfig.emplace_back(
            configField->getFullyQualifiedName(), configField->getOriginalFieldAddress(), std::move(created).value());
    }

    /// Doing a linear scan to keep the order of the values in resolved Config.
    /// If we'd add positional arguments, we just have to ensure that optional values
    /// must come after any passed argument in the order of the declared config, to avoid unintentionally passing a value as the wrong
    /// parameter because something was filled in by a default value.
    for (const auto& declaredField : declaredConfig)
    {
        if (std::ranges::contains(resolvedConfig, declaredField.getFullyQualifiedName(), &InstantiatedConfigValue::getFullyQualifiedName))
        {
            continue;
        }
        if (const auto defaultConfigValue = configDefaults.getFieldByName(declaredField.getFullyQualifiedName());
            defaultConfigValue.has_value())
        {
            auto defaultLiteral = defaultConfigValue->get();
            auto defaultValue = declaredField.apply(defaultLiteral);
            PRECONDITION(
                defaultValue.has_value(),
                "Additional default value {} for field {} could not be validated",
                std::visit([](const auto& literal) { return fmt::format("{}", literal); }, defaultLiteral),
                declaredField.getFullyQualifiedName());
            resolvedConfig.emplace_back(
                declaredField.getFullyQualifiedName(), declaredField.getOriginalFieldAddress(), std::move(defaultValue).value());
        }
        else if (declaredField.hasDefault())
        {
            resolvedConfig.emplace_back(
                declaredField.getFullyQualifiedName(), declaredField.getOriginalFieldAddress(), ExplicitAny{declaredField.getDefault()});
        }
        else
        {
            errors.missingFields.push_back(declaredField.getFullyQualifiedName());
        }
    }

    return {Schema<InstantiatedConfigValue, Ordered>{std::move(resolvedConfig)}, std::move(errors)};
}
}

std::tuple<Schema<InstantiatedConfigValue, Ordered>, ConfigResolutionErrors> resolveConfig(
    const Schema<LiteralConfigValue, Ordered>& passedConfig,
    const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig,
    const Schema<ConfigFieldDefault, Ordered>& configDefaults)
{
    return resolveConfig(
        passedConfig,
        declaredConfig,
        configDefaults,
        [&](const QualifiedIdentifier& toFind) -> std::expected<QualifiedErasedConfigField, std::vector<QualifiedIdentifier>>
        {
            const auto configField = declaredConfig.getFieldByName(toFind);
            if (not configField.has_value())
            {
                /// Calculate the colliding suffixes that caused the lookup to fail to report to the user.
                auto foundConflicts
                    = declaredConfig
                    | std::views::filter(
                          [&](const auto& configField)
                          {
                              if (std::ranges::size(configField.getFullyQualifiedName()) >= std::ranges::size(toFind))
                              {
                                  return std::ranges::all_of(
                                      std::views::zip(
                                          configField.getFullyQualifiedName() | std::views::reverse, toFind | std::views::reverse),
                                      [](const auto& idPair) { return std::get<0>(idPair) == std::get<1>(idPair); });
                              }
                              return false;
                          })
                    | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); })
                    | std::ranges::to<std::vector>();
                return std::unexpected{std::move(foundConflicts)};
            }
            return configField.value();
        });
}

std::tuple<Schema<InstantiatedConfigValue, Ordered>, ConfigResolutionErrors> resolveConfigFullyQualified(
    const Schema<LiteralConfigValue, Ordered>& passedConfig,
    const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig,
    const Schema<ConfigFieldDefault, Ordered>& configDefaults)
{
    auto qualifiedFields = declaredConfig
        | std::views::transform([](const auto& field) { return std::pair{field.getFullyQualifiedName(), field}; })
        | std::ranges::to<std::unordered_map>();

    return resolveConfig(
        passedConfig,
        declaredConfig,
        configDefaults,
        [&](const QualifiedIdentifier& toFind) -> std::expected<QualifiedErasedConfigField, std::vector<QualifiedIdentifier>>
        {
            const auto configFieldIt = qualifiedFields.find(toFind);
            if (configFieldIt == qualifiedFields.end())
            {
                return std::unexpected{std::vector<QualifiedIdentifier>{}};
            }
            return configFieldIt->second;
        });
}

std::tuple<Schema<InstantiatedConfigValue, Ordered>, ConfigResolutionErrors> applyConfigTransformations(
    Schema<InstantiatedConfigValue, Ordered> config, const Schema<ConfigFieldTransformation, Unordered>& configTransformations)
{
    std::vector<InstantiatedConfigValue> transformedConfig;
    std::vector<std::pair<QualifiedIdentifier, Exception>> failedTransformations;
    auto transformedValues
        = config
        | std::views::transform(
              [&](const auto& validatedValue) -> std::expected<InstantiatedConfigValue, std::pair<QualifiedIdentifier, Exception>>
              {
                  if (auto transformation = configTransformations.getFieldByName(validatedValue.getFullyQualifiedName()))
                  {
                      return transformation->getTransformation()(validatedValue.getErasedValue())
                          .transform_error([&](const auto& error) { return std::pair{validatedValue.getFullyQualifiedName(), error}; })
                          .transform(
                              [&](const auto& value)
                              {
                                  return InstantiatedConfigValue{
                                      validatedValue.getFullyQualifiedName(), validatedValue.getOriginalFieldAddress(), value};
                              });
                  }
                  return validatedValue;
              });

    for (auto transformedValue : transformedValues)
    {
        if (transformedValue.has_value())
        {
            transformedConfig.emplace_back(std::move(transformedValue).value());
        }
        else
        {
            failedTransformations.emplace_back(std::move(transformedValue).error());
        }
    }
    return {
        Schema<InstantiatedConfigValue, Ordered>{std::move(transformedConfig)},
        ConfigResolutionErrors{.unresolvableFields = {}, .failedInstantiations = std::move(failedTransformations), .missingFields = {}}};
}

std::expected<Schema<InstantiatedConfigValue, Ordered>, ConfigResolutionErrors>
toExpected(std::tuple<Schema<InstantiatedConfigValue, Ordered>, ConfigResolutionErrors> result)
{
    if (std::get<ConfigResolutionErrors>(result).empty())
    {
        return std::get<Schema<InstantiatedConfigValue, Ordered>>(std::move(result));
    }
    return std::unexpected(std::get<ConfigResolutionErrors>(std::move(result)));
}
}
