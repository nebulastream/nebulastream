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

bool InvalidConfigSpecification::empty() const
{
    return unresolvableFields.empty() && failedInstantiations.empty() && missingFields.empty();
}

InvalidConfigSpecification InvalidConfigSpecification::combine(InvalidConfigSpecification lhs, InvalidConfigSpecification rhs)
{
    return InvalidConfigSpecification{
        .unresolvableFields = std::array{std::move(lhs.unresolvableFields), std::move(rhs.unresolvableFields)} | std::views::join
            | std::ranges::to<std::vector>(),
        .failedInstantiations = std::array{std::move(lhs.failedInstantiations), std::move(rhs.failedInstantiations)} | std::views::join
            | std::ranges::to<std::vector>(),
        .missingFields
        = std::array{std::move(lhs.missingFields), std::move(rhs.missingFields)} | std::views::join | std::ranges::to<std::vector>()};
}

Schema<LiteralConfigValue, Ordered>
addDefaultConfigValues(const Schema<LiteralConfigValue, Ordered>& config, const Schema<ConfigFieldDefault, Ordered>& configDefaults)
{
    std::vector<LiteralConfigValue> toAdd = config | std::ranges::to<std::vector>();
    for (const auto& configDefault : configDefaults)
    {
        if (not config.getFieldByName(configDefault.getFullyQualifiedName()).has_value())
        {
            toAdd.emplace_back(configDefault.getFullyQualifiedName(), configDefault.get());
        }
    }
    return toAdd | std::ranges::to<Schema<LiteralConfigValue, Ordered>>();
}

std::tuple<Schema<ConfigValue, Ordered>, InvalidConfigSpecification> resolveConfig(
    const Schema<LiteralConfigValue, Ordered>& passedConfig,
    const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig,
    const Schema<ConfigFieldDefault, Ordered>& configDefaults)
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
            [&declaredField](const ConfigValue& exists)
            { return exists.getFullyQualifiedName() == declaredField.getFullyQualifiedName(); });
        if (alreadyResolved)
        {
            continue;
        }
        else if (declaredField.hasDefault())
        {
            resolvedConfig.emplace_back(declaredField.getFullyQualifiedName(), declaredField.getDefault());
        }
        else if (const auto defaultConfigValue = configDefaults.getFieldByName(declaredField.getFullyQualifiedName());
                 defaultConfigValue.has_value())
        {
            auto defaultLiteral = defaultConfigValue->get();
            auto defaultValue = declaredField.apply(std::move(defaultLiteral));
            PRECONDITION(
                defaultValue.has_value(),
                "Additional default value {} for field {} could not be validated",
                std::visit([](const auto& literal) { return fmt::format("{}", literal); }, defaultLiteral),
                declaredField.getFullyQualifiedName());
            resolvedConfig.emplace_back(declaredField.getFullyQualifiedName(), std::move(defaultValue).value().getValue());
        }
        else
        {
            errors.missingFields.push_back(declaredField.getFullyQualifiedName());
        }
    }

    return {Schema<ConfigValue, Ordered>{std::move(resolvedConfig)}, std::move(errors)};
}

std::tuple<Schema<ConfigValue, Ordered>, InvalidConfigSpecification>
applyConfigTransformations(Schema<ConfigValue, Ordered> config, const Schema<ConfigFieldTransformation, Unordered>& configTransformations)
{
    std::vector<ConfigValue> transformedConfig;
    std::vector<std::pair<QualifiedIdentifier, Exception>> failedTransformations;
    auto transformedValues
        = config
        | std::views::transform(
              [&](const auto& validatedValue) -> std::expected<ConfigValue, std::pair<QualifiedIdentifier, Exception>>
              {
                  if (auto transformation = configTransformations.getFieldByName(validatedValue.getFullyQualifiedName()))
                  {
                      return transformation->transformation(validatedValue.getRawValue())
                          .transform_error([&](const auto& error) { return std::pair{validatedValue.getFullyQualifiedName(), error}; })
                          .transform([&](const auto& value) { return ConfigValue{validatedValue.getFullyQualifiedName(), value}; });
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
        Schema<ConfigValue, Ordered>{std::move(transformedConfig)},
        InvalidConfigSpecification{.failedInstantiations = std::move(failedTransformations)}};
}

std::expected<Schema<ConfigValue, Ordered>, InvalidConfigSpecification>
toExpected(std::tuple<Schema<ConfigValue, Ordered>, InvalidConfigSpecification> result)
{
    if (std::get<InvalidConfigSpecification>(result).empty())
    {
        return std::get<Schema<ConfigValue, Ordered>>(std::move(result));
    }
    return std::unexpected(std::get<InvalidConfigSpecification>(std::move(result)));
}
}
