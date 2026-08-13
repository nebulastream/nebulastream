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

#include <Sinks/SinkCatalog.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <expected>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/ConfigResolution.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <OutputFormatterConfigRegistry.hpp>
#include <OutputFormatterConfigSchemaRegistry.hpp>
#include <SinkConfigRegistry.hpp>
#include <SinkConfigSchemaRegistry.hpp>

#include <Configurations/ConfigField.hpp>

namespace NES
{

SinkConfigSchema::SinkConfigSchema(
    Identifier sinkType, Identifier outputFormatterType, Schema<QualifiedErasedConfigField, Ordered> configSchema)
    : sinkType(std::move(sinkType)), outputFormatterType(std::move(outputFormatterType)), configSchema(std::move(configSchema))
{
}

SinkConfigSchema SinkConfigSchema::withConfigDefaults(Schema<ConfigFieldDefault, Ordered> configDefaults) const
{
    auto copy = *this;
    copy.configDefaults = std::move(configDefaults);
    return copy;
}

SinkConfigSchema SinkConfigSchema::withConfigTransformations(Schema<ConfigFieldTransformation, Unordered> configTransformations) const
{
    auto copy = *this;
    copy.configTransformations = std::move(configTransformations);
    return copy;
}

std::expected<std::tuple<GeneralSinkConfig, AnonymousSinkSchema, PluginSinkConfiguration, OutputFormatterDescriptor>, Exception>
SinkConfigSchema::resolveConfigs(const Schema<LiteralConfigValue, Ordered>& values) const
{
    auto [resolvedConfig, resolvationErrors] = resolveConfig(values, configSchema, configDefaults);
    auto [transformedConfig, transformationErrors] = applyConfigTransformations(resolvedConfig, configTransformations);
    auto combinedErrors = ConfigResolutionErrors::combine(std::move(resolvationErrors), std::move(transformationErrors));
    if (not combinedErrors.empty())
    {
        return std::unexpected{InvalidConfigParameter("{}", combinedErrors)};
    }
    InstantiatedConfig config{std::move(transformedConfig)};

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

    /// The NATIVE format has no registry entry and no config (see getConfigSchema).
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
    auto backpressureUpperThreshold = config.get(SinkDescriptor::BACKPRESSURE_UPPER_THRESHOLD);
    auto backpressureLowerThreshold = config.get(SinkDescriptor::BACKPRESSURE_LOWER_THRESHOLD);

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

std::expected<SinkConfigSchema, Exception> SinkCatalog::getConfigSchema(const Identifier& sinkType, const Identifier& outputFormatterType)
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

std::expected<SinkDescriptor, Exception> SinkCatalog::addSinkDescriptor(
    Identifier sinkName,
    const Schema<UnqualifiedUnboundField, Ordered>& schema,
    GeneralSinkConfig generalSinkConfig,
    PluginSinkConfiguration pluginSinkConfig,
    OutputFormatterDescriptor outputFormatterDescriptor)
{
    if (std::ranges::all_of(fmt::format("{}", sinkName), [](const char character) { return std::isdigit(character); }))
    {
        return std::unexpected{InvalidConfigParameter("Sink name '{}' is invalid: only-digit names are reserved", sinkName)};
    }
    PRECONDITION(
        generalSinkConfig.host != Host{Host::INVALID}, "A valid host must be set before creating a sink descriptor (see host policy)");

    const auto lockedSinks = sinks.wlock();
    auto sinkDescriptor = SinkDescriptor{NamedSinkDescriptor{
        sinkName,
        schema,
        std::move(generalSinkConfig.host),
        generalSinkConfig.addTimestamp,
        generalSinkConfig.backpressureUpperThreshold,
        generalSinkConfig.backpressureLowerThreshold,
        std::move(pluginSinkConfig),
        std::move(outputFormatterDescriptor)}};

    /// TODO #1504: duplicate sinks are not registered
    lockedSinks->emplace(std::move(sinkName), sinkDescriptor);
    return sinkDescriptor;
}

std::optional<SinkDescriptor> SinkCatalog::getSinkDescriptor(const Identifier& sinkName) const
{
    const auto lockedSinks = sinks.rlock();
    const auto sinkDescriptorOpt = lockedSinks->find(sinkName);
    if (sinkDescriptorOpt == lockedSinks->end())
    {
        return std::nullopt;
    }
    return sinkDescriptorOpt->second;
}

SinkDescriptor SinkCatalog::createAnonymousSinkDescriptor(
    AnonymousSinkSchema sinkSchema,
    GeneralSinkConfig generalSinkConfig,
    PluginSinkConfiguration pluginSinkConfig,
    OutputFormatterDescriptor outputFormatterDescriptor) const
{
    PRECONDITION(
        generalSinkConfig.host != Host{Host::INVALID}, "A valid host must be set before creating a sink descriptor (see host policy)");
    const auto anonymousSinkId = AnonymousSinkId{nextAnonymousSinkId.fetch_add(1)};

    return SinkDescriptor{AnonymousSinkDescriptor{
        anonymousSinkId.getRawValue(),
        std::move(sinkSchema),
        std::move(generalSinkConfig.host),
        generalSinkConfig.addTimestamp,
        generalSinkConfig.backpressureUpperThreshold,
        generalSinkConfig.backpressureLowerThreshold,
        std::move(pluginSinkConfig),
        std::move(outputFormatterDescriptor)}};
}

bool SinkCatalog::removeSinkDescriptor(const Identifier& sinkName)
{
    const auto lockedSinks = sinks.wlock();
    return lockedSinks->erase(sinkName) == 1;
}

bool SinkCatalog::removeSinkDescriptor(const SinkDescriptor& sinkDescriptor)
{
    const auto lockedSinks = sinks.wlock();
    return lockedSinks->erase(sinkDescriptor.getSinkName()) == 1;
}

bool SinkCatalog::containsSinkDescriptor(const Identifier& sinkName) const
{
    const auto lockedSinks = sinks.rlock();
    return lockedSinks->contains(sinkName);
}

bool SinkCatalog::containsSinkDescriptor(const SinkDescriptor& sinkDescriptor) const
{
    const auto lockedSinks = sinks.rlock();
    return lockedSinks->contains(sinkDescriptor.getSinkName());
}

std::vector<SinkDescriptor> SinkCatalog::getAllSinkDescriptors() const
{
    const auto lockedSinks = sinks.rlock();
    return *lockedSinks | std::ranges::views::transform([](const auto& sinkDescriptor) { return sinkDescriptor.second; })
        | std::ranges::to<std::vector>();
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
