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

#include <Sinks/SinkConfigSchema.hpp>

#include <array>
#include <cstddef>
#include <expected>
#include <memory>
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
#include <Identifiers/QualifiedIdentifier.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <OutputFormatterConfigRegistry.hpp>
#include <OutputFormatterConfigSchemaRegistry.hpp>
#include <SinkConfigRegistry.hpp>
#include <SinkConfigSchemaRegistry.hpp>

namespace NES
{

SinkConfigSchema::SinkConfigSchema(
    Identifier sinkType, Identifier outputFormatterType, Schema<QualifiedErasedConfigField, Ordered> configSchema)
    : sinkType(std::move(sinkType)), outputFormatterType(std::move(outputFormatterType)), configSchema(std::move(configSchema))
{
}

std::expected<SinkConfigSchema, Exception> SinkConfigSchema::create(const Identifier& sinkType, const Identifier& outputFormatterType)
{
    const auto sinkPluginConfigSchema = SinkConfigSchemaRegistry::instance().getSchema(sinkType.asCanonicalString());
    if (not sinkPluginConfigSchema.has_value())
    {
        return std::unexpected{
            UnknownSinkType("The sink type '{}' is not registered. If it is a plugin, make sure you activated it.", sinkType)};
    }

    /// The NATIVE format requires no output formatting and declares no config fields.
    auto formatterPluginConfigSchema = [&]() -> std::expected<Schema<QualifiedErasedConfigField, Ordered>, Exception>
    {
        if (outputFormatterType == Identifier::parse("NATIVE"))
        {
            return Schema<QualifiedErasedConfigField, Ordered>{std::vector<QualifiedErasedConfigField>{}};
        }
        const auto schema = OutputFormatterConfigSchemaRegistry::instance().getSchema(outputFormatterType.asCanonicalString());
        if (not schema.has_value())
        {
            return std::unexpected{UnknownOutputFormatterType(
                "The output formatter type '{}' is not registered. If it is a plugin, make sure you activated it.", outputFormatterType)};
        }
        return schema.value();
    }();
    if (not formatterPluginConfigSchema.has_value())
    {
        return std::unexpected{formatterPluginConfigSchema.error()};
    }

    auto targetSchema
        = std::
              array{sinkPluginConfigSchema.value(), SinkDescriptor::configSchema, std::move(formatterPluginConfigSchema).value(), OutputFormatterDescriptor::configSchema}
        | std::views::join | std::ranges::to<Schema<QualifiedErasedConfigField, Ordered>>();
    return SinkConfigSchema{sinkType, outputFormatterType, std::move(targetSchema)};
}

SinkConfigSchema SinkConfigSchema::withConfigDefaults(Schema<ConfigFieldDefault, Ordered> configDefaults) const
{
    auto copy = *this;
    copy.configDefaults = std::move(configDefaults);
    return copy;
}

std::expected<std::tuple<GeneralSinkConfig, AnonymousSinkSchema, PluginSinkConfiguration, OutputFormatterDescriptor>, Exception>
SinkConfigSchema::resolveConfigs(const Schema<LiteralConfigValue, Ordered>& values) const
{
    auto [resolvedConfig, resolutionErrors] = resolveConfig(values, configSchema, configDefaults);
    if (not resolutionErrors.empty())
    {
        return std::unexpected{InvalidConfigParameter("{}", resolutionErrors)};
    }
    InstantiatedConfig config{resolvedConfig};

    auto sinkRegistryEntry = SinkConfigRegistry::instance().find(sinkType.asCanonicalString());
    if (not sinkRegistryEntry)
    {
        return std::unexpected{
            UnknownSinkType("The sink type '{}' is not registered. If it is a plugin, make sure you activated it.", sinkType)};
    }
    auto sinkPluginConfigExp = sinkRegistryEntry->instantiate(config);
    if (not sinkPluginConfigExp)
    {
        return std::unexpected{sinkPluginConfigExp.error()};
    }

    /// The NATIVE format has no registry entry and no config (see create).
    auto outputFormatterDescriptorExp = [&]() -> std::expected<OutputFormatterDescriptor, Exception>
    {
        if (outputFormatterType == Identifier::parse("NATIVE"))
        {
            return OutputFormatterDescriptor::native();
        }
        auto outputFormatterEntry = OutputFormatterConfigRegistry::instance().find(outputFormatterType.asCanonicalString());
        if (not outputFormatterEntry)
        {
            return std::unexpected{UnknownOutputFormatterType(
                "The output formatter type '{}' is not registered. If it is a plugin, make sure you activated it.", outputFormatterType)};
        }
        auto outputFormatterConfigExp = outputFormatterEntry->instantiate(config);
        if (not outputFormatterConfigExp)
        {
            return std::unexpected{outputFormatterConfigExp.error()};
        }
        return OutputFormatterDescriptor{outputFormatterType, std::move(outputFormatterConfigExp).value()};
    }();
    if (not outputFormatterDescriptorExp)
    {
        return std::unexpected{outputFormatterDescriptorExp.error()};
    }

    PluginSinkConfiguration pluginSinkConfig{sinkType, std::move(sinkPluginConfigExp).value()};
    OutputFormatterDescriptor outputFormatterDescriptor = std::move(outputFormatterDescriptorExp).value();

    auto schema = config.get(SinkDescriptor::SCHEMA);
    auto host = config.get(SinkDescriptor::HOST);
    auto addTimestamp = config.get(SinkDescriptor::ADD_TIMESTAMP);
    auto backpressureUpperThreshold = configSchema.getFieldByName(QualifiedIdentifier::parse("BACKPRESSURE_UPPER_THRESHOLD")).has_value()
        ? config.get(SinkDescriptor::BACKPRESSURE_UPPER_THRESHOLD)
        : size_t{1000};
    auto backpressureLowerThreshold = configSchema.getFieldByName(QualifiedIdentifier::parse("BACKPRESSURE_LOWER_THRESHOLD")).has_value()
        ? config.get(SinkDescriptor::BACKPRESSURE_LOWER_THRESHOLD)
        : size_t{200};

    auto generalSinkConfig = GeneralSinkConfig{
        .host = std::move(host),
        .addTimestamp = addTimestamp,
        .backpressureUpperThreshold = backpressureUpperThreshold,
        .backpressureLowerThreshold = backpressureLowerThreshold,
    };

    auto sinkSchemaVariant = [&] -> AnonymousSinkSchema
    {
        if (not schema)
        {
            return std::monostate{};
        }
        return std::make_shared<Schema<UnqualifiedUnboundField, Ordered>>(*schema);
    }();

    return std::tuple{
        std::move(generalSinkConfig), std::move(sinkSchemaVariant), std::move(pluginSinkConfig), std::move(outputFormatterDescriptor)};
}

std::ostream& operator<<(std::ostream& os, const GeneralSinkConfig& config)
{
    return os << fmt::format(
               "GeneralSinkConfig(host: {}, addTimestamp: {}, backpressure: [{}, {}])",
               config.host,
               config.addTimestamp,
               config.backpressureLowerThreshold,
               config.backpressureUpperThreshold);
}

}
