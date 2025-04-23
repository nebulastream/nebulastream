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
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <gmock/internal/gmock-internal-utils.h>

#include <ErrorHandling.hpp>

namespace NES::Catalogs::Source
{

std::optional<LogicalSource> SourceCatalog::addLogicalSource(const std::string& logicalSourceName, const Schema& schema)
{
    std::unique_lock lock(catalogMutex);
    NES_TRACE("SourceCatalog: Check if logical source {} already exist.", logicalSourceName);
    if (!containsLogicalSource(logicalSourceName))
    {
        NES_DEBUG("SourceCatalog: add logical source {}", logicalSourceName);
        const LogicalSource logicalSource{logicalSourceName, std::make_shared<Schema>(schema)};
        namesToLogicalSourceMapping.emplace(logicalSourceName, logicalSource);
        logicalToPhysicalSourceMapping.emplace(logicalSource, std::set<Sources::SourceDescriptor>{});
        return logicalSource;
    }
    NES_ERROR("SourceCatalog: logical source {} already exists", logicalSourceName);
    return std::nullopt;
}


std::optional<Sources::SourceDescriptor> SourceCatalog::createPhysicalSource(
    Configurations::DescriptorConfig::Config&& descriptorConfig,
    const LogicalSource& logicalSource,
    WorkerId workerId,
    std::string sourceType,
    const Sources::ParserConfig& parserConfig)
{
    std::unique_lock lock(catalogMutex);

    const auto logicalPhysicalIter = logicalToPhysicalSourceMapping.find(logicalSource);
    if (logicalPhysicalIter == logicalToPhysicalSourceMapping.end())
    {
        NES_DEBUG("Trying to create physical source for logical source \"{}\" which does not exist", logicalSource.getLogicalSourceName());
    }

    NES_TRACE("SourceCatalog: trying to create new physical source of type {} on worker {}", sourceType, workerId);
    auto insertHint = idsToPhysicalSources.begin();
    if (not idsToPhysicalSources.empty())
    {
        //Select highest id + 1, but ensure that it's not used, should it overflow after some eons
        //I think we can ignore the case that there are MAX_UINT64 physical sources
        insertHint = std::ranges::prev(std::ranges::end(idsToPhysicalSources));
        while (idsToPhysicalSources.contains(insertHint->first + 1))
        {
            ++insertHint;
        }
    }
    auto id = insertHint->first + 1;
    Sources::SourceDescriptor descriptor{(std::move(descriptorConfig)), id, workerId, logicalSource, sourceType, parserConfig};
    idsToPhysicalSources.emplace_hint(std::ranges::end(idsToPhysicalSources), id, descriptor);
    logicalPhysicalIter->second.insert(descriptor);
    // physicalToLogicalSourceMappings.emplace(physicalSource, std::unordered_set<LogicalSource>{});
    NES_DEBUG(
        "SourceCatalog: successfully registered new physical source of type {} on worker {} with id {}",
        descriptor.getSourceType(),
        workerId,
        id);
    return descriptor;
}

// bool SourceCatalog::associate(const LogicalSource& logicalSource, const PhysicalSource& physicalSource)
// {
//     std::unique_lock lock(catalogMutex);
//     NES_TRACE(
//         "SourceCatalog: trying to add physical source {} to logical source \"{}\"",
//         physicalSource.getID(),
//         logicalSource.getLogicalSourceName());
//     const auto logicalToPhysicalEntry = logicalToPhysicalSourceMapping.find(logicalSource);
//     if (logicalToPhysicalEntry == logicalToPhysicalSourceMapping.end())
//     {
//         NES_WARNING(
//             "SourceCatalog: Could not add physical source {} to logical source \"{}\" because logical source is not registered",
//             physicalSource.getID(),
//             logicalSource.getLogicalSourceName());
//         return false;
//     }
//
//     const auto physicalToLogicalEntry = physicalToLogicalSourceMappings.find(physicalSource);
//     if (physicalToLogicalEntry == physicalToLogicalSourceMappings.end())
//     {
//         NES_WARNING(
//             "SourceCatalog: Could not add physical source {} to logical source \"{}\" because physical source is not registered",
//             physicalSource.getID(),
//             logicalSource.getLogicalSourceName());
//         return false;
//     }
//
//
//     auto [_1, l2pSuccess] = logicalToPhysicalEntry->second.insert(physicalSource);
//     auto [_2, p2lSuccess] = physicalToLogicalEntry->second.insert(logicalSource);
//
//     //We log if nothing new was inserted, because it might be accidental and a developer would want to know,
//     //but it is not corrupting the state in any way
//     if (not l2pSuccess)
//     {
//         NES_DEBUG(
//             "SourceCatalog: Did not add physical source {} to associations of logical source \"{}\" because it was already associated",
//             physicalSource.getID(),
//             logicalSource.getLogicalSourceName());
//     }
//     if (not p2lSuccess)
//     {
//         NES_DEBUG(
//             "SourceCatalog: Did not add logical source \"{}\" to associations of physical source {} because it was already added",
//             logicalSource.getLogicalSourceName(),
//             physicalSource.getID());
//     }
//
//     return l2pSuccess && p2lSuccess;
// }
//
//
// bool SourceCatalog::dissociate(const LogicalSource& logicalSource, const PhysicalSource& physicalSource)
// {
//     std::unique_lock lock{catalogMutex};
//     NES_DEBUG(
//         "SourceCatalog: attempting to dissociate logical source \"{}\" from physical source {}",
//         logicalSource.getLogicalSourceName(),
//         physicalSource.getID());
//
//     const auto logicalToPhysicalEntry = logicalToPhysicalSourceMapping.find(logicalSource);
//     if (logicalToPhysicalEntry == logicalToPhysicalSourceMapping.end())
//     {
//         NES_WARNING(
//             "SourceCatalog: Could not dissociate physical source {} from logical source \"{}\" because logical source is not registered",
//             physicalSource.getID(),
//             logicalSource.getLogicalSourceName());
//         return false;
//     }
//
//     const auto physicalToLogicalEntry = physicalToLogicalSourceMappings.find(physicalSource);
//     if (physicalToLogicalEntry == physicalToLogicalSourceMappings.end())
//     {
//         NES_WARNING(
//             "SourceCatalog: Could not dissociate physical source {} from logical source \"{}\" because physical source is not registered",
//             physicalSource.getID(),
//             logicalSource.getLogicalSourceName());
//         return false;
//     }
//
//     const auto erasedFromL2P = logicalToPhysicalEntry->second.erase(physicalSource);
//     const auto erasedFromP2L = physicalToLogicalEntry->second.erase(logicalSource);
//
//     if (erasedFromL2P != 0)
//     {
//         NES_DEBUG(
//             "SourceCatalog: Did not remove physical source {} from associations of logical source \"{}\" because it was not associated",
//             physicalSource.getID(),
//             logicalSource.getLogicalSourceName());
//     }
//     if (erasedFromP2L != 0)
//     {
//         NES_DEBUG(
//             "SourceCatalog: Did not remove logical source \"{}\" from associations of physical source \"{}\" because it was not associated",
//             logicalSource.getLogicalSourceName(),
//             physicalSource.getID());
//     }
// }

std::optional<LogicalSource> SourceCatalog::getLogicalSource(const std::string& logicalSourceName) const
{
    std::unique_lock lock(catalogMutex);
    if (const auto found = namesToLogicalSourceMapping.find(logicalSourceName); found != namesToLogicalSourceMapping.end())
    {
        return found->second;
    }
    return std::nullopt;
}
bool SourceCatalog::containsLogicalSource(const LogicalSource& logicalSource) const
{
    std::unique_lock lock(catalogMutex);
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
    std::unique_lock lock{catalogMutex};
    return namesToLogicalSourceMapping.contains(logicalSourceName);
}
std::optional<Sources::SourceDescriptor> SourceCatalog::getPhysicalSource(const uint64_t physicalSourceID) const
{
    std::unique_lock lock{catalogMutex};
    if (const auto physicalSourceIter = idsToPhysicalSources.find(physicalSourceID); physicalSourceIter != idsToPhysicalSources.end())
    {
        return physicalSourceIter->second;
    }
    return std::nullopt;
}
std::set<Sources::SourceDescriptor> SourceCatalog::getPhysicalSources(const LogicalSource& logicalSource) const
{
    std::unique_lock lock(catalogMutex);
    if (const auto found = logicalToPhysicalSourceMapping.find(logicalSource); found != logicalToPhysicalSourceMapping.end())
    {
        return found->second;
    }
    throw UnknownSource("{}", logicalSource.getLogicalSourceName());
}


bool SourceCatalog::removeLogicalSource(const LogicalSource& logicalSource)
{
    std::unique_lock lock(catalogMutex);
    NES_TRACE("SourceCatalog: attempting to remove logical source \"{}\"", logicalSource.getLogicalSourceName());

    //Find all associated physical sources and remove the logical source from their associations first
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
                physicalSource.getPhysicalSourceID(),
                logicalSource.getLogicalSourceName());
        }
    }

    namesToLogicalSourceMapping.erase(logicalSource.getLogicalSourceName());
    logicalToPhysicalSourceMapping.erase(logicalSource);
    NES_DEBUG("SourceCatalog: removed logical source \"{}\"", logicalSource.getLogicalSourceName());
    return true;
}

bool SourceCatalog::removePhysicalSource(const Sources::SourceDescriptor& physicalSource)
{
    std::unique_lock lock{catalogMutex};
    NES_TRACE("SourceCatalog: attempting to remove physical source {}", physicalSource.getPhysicalSourceID());
    const auto physicalSourcePair = idsToPhysicalSources.find(physicalSource.getPhysicalSourceID());
    //Verify that physical source is still registered, otherwise the invariants later don't make sense
    if (physicalSourcePair == idsToPhysicalSources.end())
    {
        NES_DEBUG("SourceCatalog: physical source {} already not present", physicalSource.getPhysicalSourceID());
        return false;
    }

    const auto physicalSourcesIter = logicalToPhysicalSourceMapping.find(physicalSource.getLogicalSource());
    if (physicalSourcesIter == logicalToPhysicalSourceMapping.end())
    {
        NES_WARNING(
            "Did not find logical source \"{}\" when trying to remove associate physical source {}",
            physicalSource.getLogicalSource().getLogicalSourceName(),
            physicalSource.getPhysicalSourceID());
    }
    if (const auto removedPhysicalFromLogical = physicalSourcesIter->second.erase(physicalSource); removedPhysicalFromLogical < 1)
    {
        NES_WARNING(
            "While removing physical source {}, associated logical source \"{}\" was not associated with it anymore",
            physicalSource.getPhysicalSourceID(),
            physicalSource.getLogicalSource().getLogicalSourceName())
    }

    idsToPhysicalSources.erase(physicalSourcePair);
    NES_DEBUG("SourceCatalog: removed physical source {}", physicalSource.getPhysicalSourceID());
    return true;
}
std::unordered_set<LogicalSource> SourceCatalog::getAllLogicalSources() const
{
    std::unique_lock lock{catalogMutex};
    return namesToLogicalSourceMapping | std::ranges::views::transform([](auto& pair) { return pair.second; })
        | ranges::to<std::unordered_set<LogicalSource>>();
}
std::unordered_map<LogicalSource, std::set<Sources::SourceDescriptor>> SourceCatalog::getAllSources() const
{
    return logicalToPhysicalSourceMapping;
}

}
