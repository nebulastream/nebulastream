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

#include <Sources/SourceConfigSchema.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/ConfigParsing.hpp>
#include <Configurations/ConfigResolution.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>
#include <InputFormatterProvider.hpp>
#include <SourceConfigRegistry.hpp>
#include <SourceConfigSchemaRegistry.hpp>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Util/Pointers.hpp>
#include <InputFormatterConfigRegistry.hpp>
#include <InputFormatterConfigSchemaRegistry.hpp>

namespace NES
{


PhysicalSourceBuilder::PhysicalSourceBuilder(
    GeneralSourceConfig generalSourceConfig,
    PluginSourceConfiguration sourcePluginConfig,
    InputFormatterDescriptor inputFormatterPluginConfig)
    : generalSourceConfig(std::move(generalSourceConfig))
    , sourcePluginConfig(std::move(sourcePluginConfig))
    , inputFormatterPluginConfig(std::move(inputFormatterPluginConfig))
{
}

std::expected<SourceDescriptor, Exception> PhysicalSourceBuilder::build(
    PhysicalSourceId physicalSourceId, Schema<UnqualifiedUnboundField, Ordered> schema, std::optional<Identifier> logicalSourceName) &&
{
    INVARIANT(!this->wasCalled, "PhysicalSourceBuilder called twice");
    this->wasCalled = true;
    return SourceDescriptor{
        physicalSourceId,
        std::move(schema),
        this->generalSourceConfig.host,
        this->generalSourceConfig.maxInflightBuffers,
        std::move(this->sourcePluginConfig),
        std::move(this->inputFormatterPluginConfig),
        std::move(logicalSourceName)};
}

SourceConfigSchema::SourceConfigSchema(
    Identifier sourceType, Identifier inputFormatterType, Schema<QualifiedErasedConfigField, Ordered> configSchema)
    : sourceType(std::move(sourceType)), inputFormatterType(std::move(inputFormatterType)), configSchema(std::move(configSchema))
{
}

SourceConfigSchema SourceConfigSchema::withConfigDefaults(Schema<ConfigFieldDefault, Ordered> configDefaults) const
{
    auto copy = *this;
    copy.configDefaults = std::move(configDefaults);
    return copy;
}

SourceConfigSchema SourceConfigSchema::withConfigTransformations(Schema<ConfigFieldTransformation, Unordered> configTransformations) const
{
    auto copy = *this;
    copy.configTransformations = std::move(configTransformations);
    return copy;
}

std::expected<
    std::tuple<
        GeneralSourceConfig,
        PluginSourceConfiguration,
        InputFormatterDescriptor,
        std::optional<Schema<UnqualifiedUnboundField, Ordered>>>,
    Exception>
SourceConfigSchema::resolveConfigs(const Schema<LiteralConfigValue, Ordered>& values) const
{
    auto [resolvedConfig, resolvationErrors] = resolveConfig(values, configSchema, configDefaults);
    auto [transformedConfig, transformationErrors] = applyConfigTransformations(resolvedConfig, configTransformations);
    auto combinedErrors = ConfigResolutionErrors::combine(std::move(resolvationErrors), std::move(transformationErrors));
    if (not combinedErrors.empty())
    {
        return std::unexpected{InvalidConfigParameter("{}", combinedErrors)};
    }

    const InstantiatedConfig config{transformedConfig};
    auto sourceRegistryEntry = SourceConfigRegistry::instance().find(sourceType.asCanonicalString());
    if (not sourceRegistryEntry.has_value())
    {
        return std::unexpected{
            UnknownSourceType("The source type '{}' is not registered. If it is a plugin, make sure you activated it.", sourceType)};
    };
    auto inputFormatterRegistryEntry = InputFormatterConfigRegistry::instance().find(inputFormatterType.asCanonicalString());
    if (not inputFormatterRegistryEntry.has_value())
    {
        return std::unexpected{UnknownInputFormatterType(
            "The input formatter type '{}' is not registered. If it is a plugin, make sure you activated it.", inputFormatterType)};
    }
    auto instantiatedPluginConfig = sourceRegistryEntry->instantiate(config);
    if (!instantiatedPluginConfig.has_value())
    {
        return std::unexpected{instantiatedPluginConfig.error()};
    }
    auto pluginSourceConfig = PluginSourceConfiguration{this->sourceType, std::move(instantiatedPluginConfig).value()};

    auto instantiatedInputFormatterConfig = inputFormatterRegistryEntry->instantiate(config);
    if (!instantiatedInputFormatterConfig.has_value())
    {
        return std::unexpected{instantiatedInputFormatterConfig.error()};
    }
    auto formatDescriptor = InputFormatterDescriptor{this->inputFormatterType, std::move(instantiatedInputFormatterConfig).value()};

    auto schema = config.get(SourceDescriptor::SCHEMA);
    auto hostOpt = config.get(SourceDescriptor::HOST);

    auto maxFlightInBuffers = config.get(SourceDescriptor::MAX_INFLIGHT_BUFFERS);

    return std::make_tuple(
        GeneralSourceConfig{.host = hostOpt, .maxInflightBuffers = maxFlightInBuffers},
        std::move(pluginSourceConfig),
        std::move(formatDescriptor),
        std::move(schema));
}

std::expected<SourceConfigSchema, Exception> SourceConfigSchema::create(const Identifier& sourceType, const Identifier& inputFormatterType)
{
    const auto sourcePluginConfigSchema = SourceConfigSchemaRegistry::instance().getSchema(sourceType.asCanonicalString());
    if (not sourcePluginConfigSchema.has_value())
    {
        return std::unexpected{
            UnknownSourceType("The source type '{}' is not registered. If it is a plugin, make sure you activated it.", sourceType)};
    }

    const auto inputFormatterPluginConfigSchema
        = InputFormatterConfigSchemaRegistry::instance().getSchema(inputFormatterType.asCanonicalString());
    if (not inputFormatterPluginConfigSchema.has_value())
    {
        return std::unexpected{UnknownInputFormatterType(
            "The input formatter type '{}' is not registered. If it is a plugin, make sure you activate it.", inputFormatterType)};
    }

    auto targetSchema
        = std::
              array{sourcePluginConfigSchema.value(), SourceDescriptor::configSchema, inputFormatterPluginConfigSchema.value(), InputFormatterDescriptor::configSchema}
        | std::views::join | std::ranges::to<Schema<QualifiedErasedConfigField, Ordered>>();
    return SourceConfigSchema{sourceType, inputFormatterType, std::move(targetSchema)};
}

std::ostream& operator<<(std::ostream& os, const GeneralSourceConfig& config)
{
    return os << fmt::format("GeneralSourceConfig(host: {}, maxInflightBuffers: {})", config.host, config.maxInflightBuffers);
}

namespace
{
/// Transitional helper for the map-based registration API: raw string maps become config
/// literals (unqualified names resolve by suffix against the merged schema) and are resolved
/// through the same SourceConfigSchema path as the typed API.
std::expected<
    std::tuple<
        GeneralSourceConfig,
        PluginSourceConfiguration,
        InputFormatterDescriptor,
        std::optional<Schema<UnqualifiedUnboundField, Ordered>>>,
    Exception>
resolveLegacyConfigMaps(
    const Identifier& sourceType,
    const Host& host,
    const std::unordered_map<Identifier, std::string>& descriptorConfig,
    const std::unordered_map<Identifier, std::string>& parserConfig)
{
    const auto& typeKey = InputFormatterDescriptor::TYPE_FIELD.getName();
    const auto typeIter = std::ranges::find_if(parserConfig, [&](const auto& entry) { return entry.first == typeKey; });
    if (typeIter == parserConfig.end())
    {
        return std::unexpected{InvalidConfigParameter("Source config does not contain input formatter type")};
    }
    auto inputFormatterTypeExp = Identifier::tryParse(typeIter->second);
    if (!inputFormatterTypeExp.has_value())
    {
        return std::unexpected{inputFormatterTypeExp.error()};
    }
    const auto inputFormatterType = std::move(inputFormatterTypeExp).value();

    std::vector<LiteralConfigValue> literals;
    literals.reserve(descriptorConfig.size() + parserConfig.size() + 1);
    literals.emplace_back(QualifiedIdentifier::create(SourceDescriptor::HOST.getName()), host.getRawValue());
    for (const auto& [key, value] : descriptorConfig)
    {
        auto parsed = parseConfigLiteral(value);
        if (not parsed.has_value())
        {
            return std::unexpected{std::move(parsed).error()};
        }
        literals.emplace_back(QualifiedIdentifier::create(key), std::move(parsed).value());
    }
    for (const auto& [key, value] : parserConfig)
    {
        /// The formatter type selects the schema; it is only a resolvable field for registered
        /// formatter plugins (the NATIVE format has none).
        if (key == typeKey)
        {
            continue;
        }
        auto parsed = parseConfigLiteral(value);
        if (not parsed.has_value())
        {
            return std::unexpected{std::move(parsed).error()};
        }
        literals.emplace_back(QualifiedIdentifier::create(key), std::move(parsed).value());
    }


    /// The NATIVE format has no registered formatter plugin (it needs no formatting and carries
    /// no config): resolve against the source-side schema only and build the descriptor directly.
    if (inputFormatterType == Identifier::parse("NATIVE"))
    {
        auto sourcePluginConfigSchema = SourceConfigSchemaRegistry::instance().getSchema(sourceType.asCanonicalString());
        if (not sourcePluginConfigSchema.has_value())
        {
            return std::unexpected{
                UnknownSourceType("The source type '{}' is not registered. If it is a plugin, make sure you activated it.", sourceType)};
        }
        auto targetSchema = std::array{sourcePluginConfigSchema.value(), SourceDescriptor::configSchema} | std::views::join
            | std::ranges::to<Schema<QualifiedErasedConfigField, Ordered>>();
        auto [resolvedConfig, resolvationErrors] = resolveConfig(createConfigLiteralSchema(std::move(literals)), targetSchema);
        if (not resolvationErrors.empty())
        {
            return std::unexpected{InvalidConfigParameter("{}", resolvationErrors)};
        }
        const InstantiatedConfig config{resolvedConfig};
        auto sourceRegistryEntry = SourceConfigRegistry::instance().find(sourceType.asCanonicalString());
        if (not sourceRegistryEntry.has_value())
        {
            return std::unexpected{UnknownSourceType("The source type '{}' is not registered.", sourceType)};
        }
        auto instantiatedPluginConfig = sourceRegistryEntry->instantiate(config);
        if (not instantiatedPluginConfig.has_value())
        {
            return std::unexpected{instantiatedPluginConfig.error()};
        }
        return std::make_tuple(
            GeneralSourceConfig{
                .host = config.get(SourceDescriptor::HOST), .maxInflightBuffers = config.get(SourceDescriptor::MAX_INFLIGHT_BUFFERS)},
            PluginSourceConfiguration{sourceType, std::move(instantiatedPluginConfig).value()},
            InputFormatterDescriptor{inputFormatterType, ExplicitAny{std::any{std::monostate{}}}},
            config.get(SourceDescriptor::SCHEMA));
    }

    auto configSchema = SourceConfigSchema::create(sourceType, inputFormatterType);
    if (not configSchema.has_value())
    {
        return std::unexpected{std::move(configSchema).error()};
    }
    literals.emplace_back(QualifiedIdentifier::create(typeKey), inputFormatterType.asCanonicalString());
    return configSchema->resolveConfigs(createConfigLiteralSchema(std::move(literals)));
}
}

/// Builds a descriptor from string config maps.
/// The catalog stores the configuration of a source as strings.
std::expected<SourceDescriptor, Exception> SourceDescriptor::create(
    PhysicalSourceId id,
    const LogicalSource& logicalSource,
    const Identifier& sourceType,
    const Host& host,
    const std::unordered_map<Identifier, std::string>& descriptorConfig,
    const std::unordered_map<Identifier, std::string>& inputFormatterConfig,
    bool isAnonymous)
{
    auto resolved = resolveLegacyConfigMaps(sourceType, host, descriptorConfig, inputFormatterConfig);
    if (!resolved)
    {
        return std::unexpected{resolved.error()};
    }
    auto [generalConfig, pluginConfig, formatter, declaredSchema] = std::move(*resolved);
    return PhysicalSourceBuilder{std::move(generalConfig), std::move(pluginConfig), std::move(formatter)}.build(
        id, *logicalSource.getSchema(), isAnonymous ? std::nullopt : std::optional{logicalSource.getLogicalSourceName()});
}

}
