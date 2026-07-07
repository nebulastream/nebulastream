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

#include <Sources/SourceCatalog.hpp>

#include <cstdint>
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <Configurations/ConfigResolution.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>
#include <InputFormatterProvider.hpp>
#include <InputFormatterValidationProvider.hpp>
#include <SourceConfigRegistry.hpp>

namespace NES
{

std::optional<LogicalSource>
SourceCatalog::addLogicalSource(const Identifier& logicalSourceName, const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    const std::unique_lock lock(catalogMutex);
    if (!containsLogicalSource(logicalSourceName))
    {
        LogicalSource logicalSource{logicalSourceName, schema};
        namesToLogicalSourceMapping.emplace(logicalSourceName, logicalSource);
        logicalToPhysicalSourceMapping.emplace(logicalSource, std::unordered_set<SourceDescriptor>{});
        NES_DEBUG("Added logical source {}", logicalSourceName);
        return logicalSource;
    }
    NES_DEBUG("Logical source {} already exists", logicalSourceName);
    return std::nullopt;
}

std::expected<SourceDescriptor, Exception> SourceCatalog::addPhysicalSource(
    const LogicalSource& logicalSource,
    const Identifier& sourceType,
    Host host,
    std::unordered_map<Identifier, ConfigLiteral> descriptorConfig,
    const std::unordered_map<Identifier, std::string>& parserConfig)
{
    const std::unique_lock lock(catalogMutex);

    const auto logicalPhysicalIter = logicalToPhysicalSourceMapping.find(logicalSource);
    if (logicalPhysicalIter == logicalToPhysicalSourceMapping.end())
    {
        NES_DEBUG("Trying to create physical source for logical source \"{}\" which does not exist.", logicalSource.getLogicalSourceName());
        return std::unexpected{UnknownSourceName("Logical source {} does not exist.", logicalSource.getLogicalSourceName())};
    }
    auto id = PhysicalSourceId{nextPhysicalSourceId.fetch_add(1)};
    const auto declaredSchema = SourceValidationProvider::provide(sourceType.asCanonicalString());
    if (not declaredSchema.has_value())
    {
        return std::unexpected{
            UnknownSourceType("The source type '{}' is not registered. If it is a plugin, make sure you activated it.", sourceType)};
    }
    auto instantiatedConfig = instantiateConfig(descriptorConfig, *declaredSchema);
    if (not instantiatedConfig.has_value())
    {
        return std::unexpected{InvalidConfigParameter("Invalid config for source type '{}': {}", sourceType, instantiatedConfig.error())};
    }
    const auto* sourceConfigEntry = SourceConfigRegistry::instance().find(sourceType.asCanonicalString());
    if (sourceConfigEntry == nullptr)
    {
        return std::unexpected{
            UnknownSourceType("The source type '{}' declares a config schema but no SourceConfig registry entry.", sourceType)};
    }

    std::unordered_map<std::string, std::string> parserConfigStringMap;
    parserConfigStringMap.reserve(parserConfig.size());
    for (const auto& [key, value] : parserConfig)
    {
        parserConfigStringMap.emplace(key.asCanonicalString(), value);
    }
    if (not parserConfigStringMap.contains(InputFormatterDescriptor::getTypeString()))
    {
        return std::unexpected{InvalidConfigParameter("Source config does not contain input formatter type")};
    }
    const std::string inputFormat = parserConfigStringMap.at(InputFormatterDescriptor::getTypeString());
    const auto parserConfigObject = inputFormat == "NATIVE" ? DescriptorConfig::Config{}
                                                            : InputFormatterValidationProvider::provide(inputFormat, parserConfigStringMap);
    if (not parserConfigObject.has_value())
    {
        return std::unexpected{UnknownSourceType(
            "The input formatter type '{}' is not registered. If it is a plugin, make sure you activate it.", inputFormat)};
    }
    const InputFormatterDescriptor formatDescriptor{inputFormat, parserConfigObject.value()};

    auto pluginData = sourceConfigEntry->instantiate(*instantiatedConfig);
    const auto maxInflightBuffers = instantiatedConfig->get(SourceDescriptor::MAX_INFLIGHT_BUFFERS);
    SourceDescriptor descriptor{
        id,
        logicalSource,
        sourceType.asCanonicalString(),
        std::move(host),
        maxInflightBuffers,
        std::move(instantiatedConfig).value(),
        std::move(pluginData),
        formatDescriptor};
    idsToPhysicalSources.emplace(id, descriptor);
    logicalPhysicalIter->second.insert(descriptor);
    NES_DEBUG("Successfully registered new physical source of type {} with id {}", descriptor.getSourceType(), id);
    return descriptor;
}

std::optional<LogicalSource> SourceCatalog::getLogicalSource(const Identifier& logicalSourceName) const
{
    const std::unique_lock lock(catalogMutex);
    if (const auto found = namesToLogicalSourceMapping.find(logicalSourceName); found != namesToLogicalSourceMapping.end())
    {
        return found->second;
    }
    return std::nullopt;
}

bool SourceCatalog::containsLogicalSource(const LogicalSource& logicalSource) const
{
    const std::unique_lock lock(catalogMutex);
    if (const auto found = namesToLogicalSourceMapping.find(logicalSource.getLogicalSourceName());
        found != namesToLogicalSourceMapping.end())
    {
        const auto equals = found->second == logicalSource;
        {
            NES_DEBUG("Found logical source with the same name \"{}\" but different schema", logicalSource.getLogicalSourceName());
        }
        return equals;
    }
    return false;
}

bool SourceCatalog::containsLogicalSource(const Identifier& logicalSourceName) const
{
    const std::unique_lock lock{catalogMutex};
    return namesToLogicalSourceMapping.contains(logicalSourceName);
}

std::optional<SourceDescriptor> SourceCatalog::getPhysicalSource(const PhysicalSourceId physicalSourceID) const
{
    const std::unique_lock lock{catalogMutex};
    if (const auto physicalSourceIter = idsToPhysicalSources.find(physicalSourceID); physicalSourceIter != idsToPhysicalSources.end())
    {
        return physicalSourceIter->second;
    }
    return std::nullopt;
}

std::optional<SourceDescriptor> SourceCatalog::getInlineSource(
    const Identifier& sourceType,
    const Schema<UnqualifiedUnboundField, Ordered>& schema,
    Host host,
    const std::unordered_map<Identifier, std::string>& parserConfigMap,
    std::unordered_map<Identifier, ConfigLiteral> sourceConfigMap) const
{
    const auto declaredSchema = SourceValidationProvider::provide(sourceType.asCanonicalString());
    if (not declaredSchema.has_value())
    {
        return std::nullopt;
    }
    auto instantiatedConfig = instantiateConfig(sourceConfigMap, *declaredSchema);
    if (not instantiatedConfig.has_value())
    {
        NES_ERROR("Invalid config for inline source of type '{}': {}", sourceType, instantiatedConfig.error());
        return std::nullopt;
    }
    const auto* sourceConfigEntry = SourceConfigRegistry::instance().find(sourceType.asCanonicalString());
    if (sourceConfigEntry == nullptr)
    {
        return std::nullopt;
    }

    std::unordered_map<std::string, std::string> parserConfigStringMap;
    parserConfigStringMap.reserve(parserConfigMap.size());
    for (const auto& [key, value] : parserConfigMap)
    {
        parserConfigStringMap.emplace(key.asCanonicalString(), value);
    }
    if (not parserConfigStringMap.contains(InputFormatterDescriptor::getTypeString()))
    {
        throw InvalidConfigParameter("Source config does not contain input formatter type");
    }
    const std::string inputFormat = parserConfigStringMap.at(InputFormatterDescriptor::getTypeString());
    const auto parserConfigObject = inputFormat == "NATIVE" ? DescriptorConfig::Config{}
                                                            : InputFormatterValidationProvider::provide(inputFormat, parserConfigStringMap);
    if (not parserConfigObject.has_value())
    {
        throw UnknownSourceType(
            "The input formatter type '{}' is not registered. If it is a plugin, make sure you activate it.", inputFormat);
    }
    const InputFormatterDescriptor formatDescriptor{inputFormat, parserConfigObject.value()};


    auto physicalId = PhysicalSourceId{nextPhysicalSourceId.fetch_add(1)};
    auto name = Identifier::parse(physicalId.toString());

    const auto logicalSource = LogicalSource{name, schema};
    auto pluginData = sourceConfigEntry->instantiate(*instantiatedConfig);
    const auto maxInflightBuffers = instantiatedConfig->get(SourceDescriptor::MAX_INFLIGHT_BUFFERS);
    SourceDescriptor sourceDescriptor{
        physicalId,
        logicalSource,
        sourceType.asCanonicalString(),
        std::move(host),
        maxInflightBuffers,
        std::move(instantiatedConfig).value(),
        std::move(pluginData),
        formatDescriptor};
    return sourceDescriptor;
}

std::optional<std::unordered_set<SourceDescriptor>> SourceCatalog::getPhysicalSources(const LogicalSource& logicalSource) const
{
    const std::unique_lock lock(catalogMutex);
    if (const auto found = logicalToPhysicalSourceMapping.find(logicalSource); found != logicalToPhysicalSourceMapping.end())
    {
        return found->second;
    }
    return std::nullopt;
}

bool SourceCatalog::removeLogicalSource(const LogicalSource& logicalSource)
{
    const std::unique_lock lock(catalogMutex);
    if (const auto removedByName = namesToLogicalSourceMapping.erase(logicalSource.getLogicalSourceName()); removedByName == 0)
    {
        NES_TRACE("Trying to remove logical source \"{}\", but it was not registered by name", logicalSource.getLogicalSourceName());
        return false;
    }

    /// Remove physical sources associated with logical source
    const auto physicalSourcesIter = logicalToPhysicalSourceMapping.find(logicalSource);
    INVARIANT(
        physicalSourcesIter != logicalToPhysicalSourceMapping.end(),
        "Logical source \"{}\" was registered, but no entry in logicalToPhysicalSourceMappings was found",
        logicalSource.getLogicalSourceName());
    for (const auto& physicalSource : physicalSourcesIter->second)
    {
        const auto erasedPhysicalSource = idsToPhysicalSources.erase(physicalSource.physicalSourceId);
        INVARIANT(
            erasedPhysicalSource == 1,
            "Physical source {} was mapped to logical source \"{}\", but physical source did not have an entry in "
            "idsToPhysicalSources",
            physicalSource.getPhysicalSourceId(),
            logicalSource.getLogicalSourceName());
    }

    logicalToPhysicalSourceMapping.erase(logicalSource);
    NES_DEBUG("Removed logical source \"{}\"", logicalSource.getLogicalSourceName());
    return true;
}

bool SourceCatalog::removePhysicalSource(const SourceDescriptor& physicalSource)
{
    const std::unique_lock lock{catalogMutex};
    const auto physicalSourcePair = idsToPhysicalSources.find(physicalSource.getPhysicalSourceId());
    /// Verify that physical source is still registered, otherwise the invariants later don't make sense
    if (physicalSourcePair == idsToPhysicalSources.end())
    {
        NES_DEBUG("Trying to remove physical source {}, but it is not registered", physicalSource.getPhysicalSourceId());
        return false;
    }

    const auto physicalSourcesIter = logicalToPhysicalSourceMapping.find(physicalSource.getLogicalSource());
    INVARIANT(
        physicalSourcesIter != logicalToPhysicalSourceMapping.end(),
        "Did not find logical source \"{}\" when trying to remove associate physical source {}",
        physicalSource.getLogicalSource().getLogicalSourceName(),
        physicalSource.getPhysicalSourceId());

    const auto removedPhysicalFromLogical = physicalSourcesIter->second.erase(physicalSource);
    INVARIANT(
        removedPhysicalFromLogical == 1,
        "While removing physical source {}, associated logical source \"{}\" was not associated with it anymore",
        physicalSource.getPhysicalSourceId(),
        physicalSource.getLogicalSource().getLogicalSourceName());

    idsToPhysicalSources.erase(physicalSourcePair);
    NES_DEBUG("Removed physical source {}", physicalSource.getPhysicalSourceId());
    return true;
}

std::unordered_set<LogicalSource> SourceCatalog::getAllLogicalSources() const
{
    const std::unique_lock lock{catalogMutex};
    return namesToLogicalSourceMapping | std::ranges::views::transform([](auto& pair) { return pair.second; })
        | std::ranges::to<std::unordered_set<LogicalSource>>();
}

std::unordered_map<LogicalSource, std::unordered_set<SourceDescriptor>> SourceCatalog::getLogicalToPhysicalSourceMapping() const
{
    const std::unique_lock lock{catalogMutex};
    return logicalToPhysicalSourceMapping;
}

}
