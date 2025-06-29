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

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>

#include <Configurations/Descriptor.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

std::optional<LogicalSource> SourceCatalog::addLogicalSource(const std::string& logicalSourceName, const Schema& schema)
{
    const auto sharedSchema = std::make_shared<Schema>();
    for (const auto& field : schema.getFields())
    {
        sharedSchema->addField(logicalSourceName + Schema::ATTRIBUTE_NAME_SEPARATOR + field.name, field.dataType);
        if (field.name.find(logicalSourceName) != std::string::npos)
        {
            NES_DEBUG(
                "Trying to register logical source \"{}\" where passed field name {} already contained the source name as a prefix, which "
                "is probably a mistake",
                logicalSourceName,
                field.name);
        }
    }
    const std::unique_lock lock(catalogMutex);
    if (!containsLogicalSource(logicalSourceName))
    {
        LogicalSource logicalSource{logicalSourceName, sharedSchema};
        namesToLogicalSourceMapping.emplace(logicalSourceName, logicalSource);
        logicalToPhysicalSourceMapping.emplace(logicalSource, std::unordered_set<SourceDescriptor>{});
        NES_DEBUG("Added logical source {}", logicalSourceName);
        return logicalSource;
    }
    NES_DEBUG("Logical source {} already exists", logicalSourceName);
    return std::nullopt;
}

std::optional<SourceDescriptor> SourceCatalog::addPhysicalSource(
    const LogicalSource& logicalSource,
    WorkerId workerId,
    const std::string& sourceType,
    const int buffersInLocalPool,
    Configurations::DescriptorConfig::Config&& descriptorConfig,
    const ParserConfig& parserConfig)
{
    const std::unique_lock lock(catalogMutex);

    const auto logicalPhysicalIter = logicalToPhysicalSourceMapping.find(logicalSource);
    if (logicalPhysicalIter == logicalToPhysicalSourceMapping.end())
    {
        NES_DEBUG("Trying to create physical source for logical source \"{}\" which does not exist", logicalSource.getLogicalSourceName());
        return std::nullopt;
    }

    auto id = PhysicalSourceId{nextPhysicalSourceId.fetch_add(1)};
    SourceDescriptor descriptor{logicalSource, id, workerId, sourceType, buffersInLocalPool, (std::move(descriptorConfig)), parserConfig};
    idsToPhysicalSources.emplace(id, descriptor);
    logicalPhysicalIter->second.insert(descriptor);
    NES_DEBUG("Successfully registered new physical source of type {} on worker {} with id {}", descriptor.getSourceType(), workerId, id);
    return descriptor;
}

std::optional<LogicalSource> SourceCatalog::getLogicalSource(const std::string& logicalSourceName) const
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

bool SourceCatalog::containsLogicalSource(const std::string& logicalSourceName) const
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
