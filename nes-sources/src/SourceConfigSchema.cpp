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

#include <any>
#include <array>
#include <expected>
#include <optional>
#include <ostream>
#include <ranges>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/ConfigResolution.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Any.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <InputFormatterConfigRegistry.hpp>
#include <InputFormatterConfigSchemaRegistry.hpp>
#include <InputFormatterDescriptor.hpp>
#include <SourceConfigRegistry.hpp>
#include <SourceConfigSchemaRegistry.hpp>

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

std::expected<SourceConfigSchema, Exception> SourceConfigSchema::create(const Identifier& sourceType, const Identifier& inputFormatterType)
{
    const auto sourcePluginConfigSchema = SourceConfigSchemaRegistry::instance().getSchema(sourceType.asCanonicalString());
    if (not sourcePluginConfigSchema.has_value())
    {
        return std::unexpected{
            UnknownSourceType("The source type '{}' is not registered. If it is a plugin, make sure you activated it.", sourceType)};
    }

    /// NATIVE has no formatter config fields beyond INPUT_FORMATTER.TYPE.
    const auto inputFormatterPluginConfigSchema = [&]() -> std::expected<Schema<QualifiedErasedConfigField, Ordered>, Exception>
    {
        if (inputFormatterType == Identifier::parse("NATIVE"))
        {
            return Schema<QualifiedErasedConfigField, Ordered>{std::vector<QualifiedErasedConfigField>{}};
        }
        const auto schema = InputFormatterConfigSchemaRegistry::instance().getSchema(inputFormatterType.asCanonicalString());
        if (not schema.has_value())
        {
            return std::unexpected{UnknownInputFormatterType(
                "The input formatter type '{}' is not registered. If it is a plugin, make sure you activate it.", inputFormatterType)};
        }
        return schema.value();
    }();
    if (not inputFormatterPluginConfigSchema.has_value())
    {
        return std::unexpected{inputFormatterPluginConfigSchema.error()};
    }

    auto targetSchema
        = std::
              array{sourcePluginConfigSchema.value(), SourceDescriptor::configSchema, inputFormatterPluginConfigSchema.value(), InputFormatterDescriptor::configSchema}
        | std::views::join | std::ranges::to<Schema<QualifiedErasedConfigField, Ordered>>();
    return SourceConfigSchema{sourceType, inputFormatterType, std::move(targetSchema)};
}

SourceConfigSchema SourceConfigSchema::withConfigDefaults(Schema<ConfigFieldDefault, Ordered> configDefaults) const
{
    auto copy = *this;
    copy.configDefaults = std::move(configDefaults);
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
    auto [resolvedConfig, resolutionErrors] = resolveConfig(values, configSchema, configDefaults);
    if (not resolutionErrors.empty())
    {
        return std::unexpected{InvalidConfigParameter("{}", resolutionErrors)};
    }

    const InstantiatedConfig config{resolvedConfig};
    auto sourceRegistryEntry = SourceConfigRegistry::instance().find(sourceType.asCanonicalString());
    if (not sourceRegistryEntry.has_value())
    {
        return std::unexpected{
            UnknownSourceType("The source type '{}' is not registered. If it is a plugin, make sure you activated it.", sourceType)};
    };
    auto instantiatedPluginConfig = sourceRegistryEntry->instantiate(config);
    if (!instantiatedPluginConfig.has_value())
    {
        return std::unexpected{instantiatedPluginConfig.error()};
    }
    auto pluginSourceConfig = PluginSourceConfiguration{this->sourceType, std::move(instantiatedPluginConfig).value()};

    /// NATIVE forwards buffers without formatting, so it has no registry entry or formatter config.
    auto formatDescriptor = [&]() -> std::expected<InputFormatterDescriptor, Exception>
    {
        if (inputFormatterType == Identifier::parse("NATIVE"))
        {
            return InputFormatterDescriptor{inputFormatterType, ExplicitAny{std::any{std::monostate{}}}};
        }
        const auto inputFormatterRegistryEntry = InputFormatterConfigRegistry::instance().find(inputFormatterType.asCanonicalString());
        if (not inputFormatterRegistryEntry.has_value())
        {
            return std::unexpected{UnknownInputFormatterType(
                "The input formatter type '{}' is not registered. If it is a plugin, make sure you activated it.", inputFormatterType)};
        }
        auto instantiatedInputFormatterConfig = inputFormatterRegistryEntry->instantiate(config);
        if (!instantiatedInputFormatterConfig.has_value())
        {
            return std::unexpected{instantiatedInputFormatterConfig.error()};
        }
        return InputFormatterDescriptor{inputFormatterType, std::move(instantiatedInputFormatterConfig).value()};
    }();
    if (!formatDescriptor.has_value())
    {
        return std::unexpected{formatDescriptor.error()};
    }

    auto schema = config.get(SourceDescriptor::SCHEMA);
    auto hostOpt = config.get(SourceDescriptor::HOST);
    auto maxFlightInBuffers = config.get(SourceDescriptor::MAX_INFLIGHT_BUFFERS);

    return std::make_tuple(
        GeneralSourceConfig{.host = hostOpt, .maxInflightBuffers = maxFlightInBuffers},
        std::move(pluginSourceConfig),
        std::move(formatDescriptor).value(),
        std::move(schema));
}

std::ostream& operator<<(std::ostream& os, const GeneralSourceConfig& config)
{
    return os << fmt::format("GeneralSourceConfig(host: {}, maxInflightBuffers: {})", config.host, config.maxInflightBuffers);
}

}
