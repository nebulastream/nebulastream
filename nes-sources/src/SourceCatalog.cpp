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

#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include "Sources/SourceProvider.hpp"
#include "Sources/SourceValidationProvider.hpp"

#include <Configurations/Descriptor.hpp>
#include <ErrorHandling.hpp>

namespace NES::Catalogs::Source
{

std::optional<LogicalSource> SourceCatalog::addLogicalSource(const std::string& logicalSourceName, const Schema& schema)
{
    const std::unique_lock lock(catalogMutex);
    NES_TRACE("SourceCatalog: Check if logical source {} already exist.", logicalSourceName);
    if (!containsLogicalSource(logicalSourceName))
    {
        NES_DEBUG("SourceCatalog: add logical source {}", logicalSourceName);
        LogicalSource logicalSource{logicalSourceName, std::make_shared<Schema>(schema)};
        namesToLogicalSourceMapping.emplace(logicalSourceName, logicalSource);
        logicalToPhysicalSourceMapping.emplace(logicalSource, std::unordered_set<Sources::SourceDescriptor>{});
        return logicalSource;
    }
    NES_DEBUG("SourceCatalog: logical source {} already exists", logicalSourceName);
    return std::nullopt;
}


std::optional<Sources::SourceDescriptor> SourceCatalog::addPhysicalSource(
    const LogicalSource& logicalSource,
    std::string sourceType,
    WorkerId workerId,
    const std::unordered_map<std::string, std::string>& sourceConfig,
    const Sources::ParserConfig& parserConfig)
{
    const std::unique_lock lock(catalogMutex);

    const auto logicalPhysicalIter = logicalToPhysicalSourceMapping.find(logicalSource);
    if (logicalPhysicalIter == logicalToPhysicalSourceMapping.end())
    {
        NES_DEBUG("Trying to create physical source for logical source \"{}\" which does not exist", logicalSource.getLogicalSourceName());
        return std::nullopt;
    }
    auto buffersInLocalPool = Sources::SourceDescriptor::INVALID_BUFFERS_IN_LOCAL_POOL;

    if (const auto buffersInLocalPoolIter = sourceConfig.find("buffersInLocalPool"); buffersInLocalPoolIter != sourceConfig.end())
    {
        buffersInLocalPool = std::stoi(buffersInLocalPoolIter->second);
    }

    /// Copy for now, until the descriptor configs take const references
    auto descriptorConfigCopy = sourceConfig;
    auto validatedConfig = Sources::SourceValidationProvider::provide(sourceType, std::move(descriptorConfigCopy));

    NES_TRACE("SourceCatalog: trying to create new physical source of type {} on worker {}", sourceType, workerId);
    auto id = nextPhysicaSourceID.fetch_add(1);
    Sources::SourceDescriptor descriptor{
        logicalSource, id, workerId, sourceType, buffersInLocalPool, std::move(validatedConfig), parserConfig};
    idsToPhysicalSources.emplace(id, descriptor);
    logicalPhysicalIter->second.insert(descriptor);
    NES_DEBUG(
        "SourceCatalog: successfully registered new physical source of type {} on worker {} with id {}",
        descriptor.getSourceType(),
        workerId,
        id);
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
        if (found->second == logicalSource)
        {
            if (found->second.getSchema().get() != logicalSource.getSchema().get())
            {
                NES_WARNING(
                    "SourceCatalog: Catalog contains equivalent logical source \"{}\", but schemas are allocated separately",
                    logicalSource.getLogicalSourceName());
            }
            return true;
        }
    }
    return false;
}
bool SourceCatalog::containsLogicalSource(const std::string& logicalSourceName) const
{
    const std::unique_lock lock{catalogMutex};
    return namesToLogicalSourceMapping.contains(logicalSourceName);
}
std::optional<Sources::SourceDescriptor> SourceCatalog::getPhysicalSource(const uint64_t physicalSourceID) const
{
    const std::unique_lock lock{catalogMutex};
    if (const auto physicalSourceIter = idsToPhysicalSources.find(physicalSourceID); physicalSourceIter != idsToPhysicalSources.end())
    {
        return physicalSourceIter->second;
    }
    return std::nullopt;
}
bool SourceCatalog::containsPhysicalSource(const uint64_t physicalSourceId) const
{
    const std::unique_lock lock{catalogMutex};
    return idsToPhysicalSources.contains(physicalSourceId);
}
std::optional<std::unordered_set<Sources::SourceDescriptor>> SourceCatalog::getPhysicalSources(const LogicalSource& logicalSource) const
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
    NES_TRACE("SourceCatalog: attempting to remove logical source \"{}\"", logicalSource.getLogicalSourceName());

    if (const auto removedByName = namesToLogicalSourceMapping.erase(logicalSource.getLogicalSourceName()); removedByName == 0)
    {
        NES_TRACE(
            "SourceCatalog: trying to remove logical source \"{}\", but it was not registered by name",
            logicalSource.getLogicalSourceName());
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
        if (const auto erasedPhysicalSource = idsToPhysicalSources.erase(physicalSource.physicalSourceID); erasedPhysicalSource == 0)
        {
            NES_DEBUG(
                "Physical source {} was mapped to logical source \"{}\", but physical source did not have an entry in "
                "idsToPhysicalSources",
                physicalSource.getPhysicalSourceId(),
                logicalSource.getLogicalSourceName());
        }
    }

    logicalToPhysicalSourceMapping.erase(logicalSource);
    NES_DEBUG("SourceCatalog: removed logical source \"{}\"", logicalSource.getLogicalSourceName());
    return true;
}

bool SourceCatalog::removePhysicalSource(const Sources::SourceDescriptor& physicalSource)
{
    const std::unique_lock lock{catalogMutex};
    NES_TRACE("SourceCatalog: attempting to remove physical source {}", physicalSource.getPhysicalSourceId());
    const auto physicalSourcePair = idsToPhysicalSources.find(physicalSource.getPhysicalSourceId());
    /// Verify that physical source is still registered, otherwise the invariants later don't make sense
    if (physicalSourcePair == idsToPhysicalSources.end())
    {
        NES_DEBUG("SourceCatalog: physical source {} already not present", physicalSource.getPhysicalSourceId());
        return false;
    }

    const auto physicalSourcesIter = logicalToPhysicalSourceMapping.find(physicalSource.getLogicalSource());
    if (physicalSourcesIter == logicalToPhysicalSourceMapping.end())
    {
        NES_WARNING(
            "Did not find logical source \"{}\" when trying to remove associate physical source {}",
            physicalSource.getLogicalSource().getLogicalSourceName(),
            physicalSource.getPhysicalSourceId());
    }
    if (const auto removedPhysicalFromLogical = physicalSourcesIter->second.erase(physicalSource); removedPhysicalFromLogical < 1)
    {
        NES_WARNING(
            "While removing physical source {}, associated logical source \"{}\" was not associated with it anymore",
            physicalSource.getPhysicalSourceId(),
            physicalSource.getLogicalSource().getLogicalSourceName())
    }

    idsToPhysicalSources.erase(physicalSourcePair);
    NES_DEBUG("SourceCatalog: removed physical source {}", physicalSource.getPhysicalSourceId());
    return true;
}
std::unordered_set<LogicalSource> SourceCatalog::getAllLogicalSources() const
{
    const std::unique_lock lock{catalogMutex};
    return namesToLogicalSourceMapping | std::ranges::views::transform([](auto& pair) { return pair.second; })
        | std::ranges::to<std::unordered_set<LogicalSource>>();
}
std::unordered_map<LogicalSource, std::unordered_set<Sources::SourceDescriptor>> SourceCatalog::getAllSources() const
{
    const std::unique_lock lock{catalogMutex};
    return logicalToPhysicalSourceMapping;
}

}
