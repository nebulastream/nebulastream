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

#include <Configurations/SQLConfigValue.hpp>

namespace NES
{

std::ostream& operator<<(std::ostream& os, const InvalidSQLConfigSpecification& errors)
{
    os << "Invalid SQL Config Specification:" << std::endl;
    if (not errors.unresolvableFields.empty())
    {
        os << "Unresolvable fields: " << std::endl;
        os << fmt::format("{}", fmt::join(errors.unresolvableFields, ", ")) << std::endl;
    }
    if (not errors.failedInstantiations.empty())
    {
        os << "Failed instantiations: " << std::endl;
        os << fmt::format(
            "{}",
            fmt::join(
                errors.failedInstantiations | std::views::transform([](const auto& exception) { return exception.second.what(); }), ", "))
           << std::endl;
    }
    if (not errors.missingFields.empty())
    {
        os << "Missing fields: " << std::endl;
        os << fmt::format("{}", fmt::join(errors.missingFields, ", ")) << std::endl;
    }
    return os;
}

std::expected<Schema<ConfigValue, Ordered>, InvalidSQLConfigSpecification>
resolveConfig(Schema<SQLConfigValue, Ordered> passedConfig, Schema<QualifiedErasedConfigField, Ordered> declaredConfig)
{
    std::vector<QualifiedIdentifier> unresolvableFields;
    std::vector<std::pair<QualifiedIdentifier, Exception>> failedInstantiations;
    std::vector<QualifiedIdentifier> missingFields;
    std::vector<ConfigValue> resolvedConfig;

    for (const auto& passedValue : passedConfig)
    {
        const auto configField = declaredConfig.getFieldByName(passedValue.getFullyQualifiedName());
        if (not configField.has_value())
        {
            unresolvableFields.push_back(passedValue.getFullyQualifiedName());
        }
        auto created = configField->apply(passedValue.getValue());
        if (not created.has_value())
        {
            failedInstantiations.push_back(std::make_pair(passedValue.getFullyQualifiedName(), created.error()));
        }
        auto value = ConfigValue{configField->getFullyQualifiedName(), std::move(created).value()};
        resolvedConfig.push_back(std::move(value));
    }

    /// I don't want to use an unordered_map<QualifiedIdentifier, ConfigValue> for resolved config just to avoid this n x m comparison to keep the order of the values in resolved Config.
    /// If we'd add positional arguments, we just have to ensure that optional values must come after any passed argument in the order of the declared config,
    /// to avoid unintentionally passing a value as the wrong parameter because something was filled in by a default value.
    for (const auto& declaredField : declaredConfig)
    {
        if (auto it = std::ranges::find_if(
                resolvedConfig,
                [&declaredField](const ConfigValue& exists)
                { return exists.getFullyQualifiedName() == declaredField.getFullyQualifiedName(); });
            it == resolvedConfig.end())
        {
            if (declaredField.hasDefault())
            {
                resolvedConfig.push_back(ConfigValue{declaredField.getFullyQualifiedName(), declaredField.getDefault()});
            }
            missingFields.push_back(declaredField.getFullyQualifiedName());
        }
    }
    if (not unresolvableFields.empty() or not failedInstantiations.empty() or not missingFields.empty())
    {
        return std::unexpected{InvalidSQLConfigSpecification{unresolvableFields, failedInstantiations, missingFields}};
    }
    return Schema<ConfigValue, Ordered>{std::move(resolvedConfig)};
}

}
