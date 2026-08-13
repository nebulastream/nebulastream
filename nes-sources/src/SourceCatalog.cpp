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

#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceConfigSchema.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES
{

SourceCatalog::SourceCatalog(Private)
{
}

SharedPtr<SourceCatalog> SourceCatalog::create()
{
    return std::make_shared<SourceCatalog>(Private{});
}

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

SourceDescriptor SourceCatalog::createAnonymousSourceDescriptor(
    Schema<UnqualifiedUnboundField, Ordered> schema,
    GeneralSourceConfig generalConfig,
    PluginSourceConfiguration pluginConfig,
    InputFormatterDescriptor inputFormatter) const
{
    return SourceDescriptor{
        PhysicalSourceId{nextPhysicalSourceId.fetch_add(1)},
        std::move(schema),
        std::move(generalConfig.host),
        generalConfig.maxInflightBuffers,
        std::move(pluginConfig),
        std::move(inputFormatter),
        std::nullopt};
}

std::expected<SourceDescriptor, Exception>
SourceCatalog::registerWithLogicalSource(PhysicalSourceBuilder builder, const Identifier& logicalSourceName)
{
    const std::unique_lock lock(catalogMutex);
    const auto logicalSourceIter = namesToLogicalSourceMapping.find(logicalSourceName);
    if (logicalSourceIter == namesToLogicalSourceMapping.end())
    {
        return std::unexpected{UnknownSourceName("Logical source {} does not exist.", logicalSourceName)};
    }
    const auto logicalPhysicalIter = logicalToPhysicalSourceMapping.find(logicalSourceIter->second);
    PRECONDITION(
        logicalPhysicalIter != logicalToPhysicalSourceMapping.end(),
        "Source catalog corrupted, logical source name existed, but no mapping to physical sources found");

    return std::move(builder)
        .build(PhysicalSourceId{nextPhysicalSourceId.fetch_add(1)}, *logicalSourceIter->second.getSchema(), logicalSourceName)
        .transform(
            [this, &logicalPhysicalIter](SourceDescriptor descriptor)
            {
                auto [iter, success] = logicalPhysicalIter->second.insert(descriptor);
                PRECONDITION(
                    success,
                    "Couldn't insert new source descriptor into logical source mapping, the uniqueness of physical source IDs must have "
                    "been violated");
                idsToPhysicalSources.emplace(descriptor.getPhysicalSourceId(), descriptor);
                return descriptor;
            });
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
    INVARIANT(
        physicalSource.getLogicalSourceName().has_value(), "Physical source id was registered but was not attached to logical source");
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access) - guarded by the INVARIANT above
    const auto logicalSourceIter = namesToLogicalSourceMapping.find(physicalSource.getLogicalSourceName().value());
    INVARIANT(
        logicalSourceIter != namesToLogicalSourceMapping.end(),
        "Did not find logical source \"{}\" when trying to remove physical source {}",
        physicalSource.getLogicalSourceName().value(), /// NOLINT(bugprone-unchecked-optional-access) - guarded by the INVARIANT above
        physicalSource.getPhysicalSourceId());

    const auto physicalSourcesIter = logicalToPhysicalSourceMapping.find(logicalSourceIter->second);
    INVARIANT(
        physicalSourcesIter != logicalToPhysicalSourceMapping.end(),
        "Did not find logical source \"{}\" when trying to remove associate physical source {}",
        logicalSourceIter->second.getLogicalSourceName(),
        physicalSource.getPhysicalSourceId());

    const auto removedPhysicalFromLogical = physicalSourcesIter->second.erase(physicalSource);
    INVARIANT(
        removedPhysicalFromLogical == 1,
        "While removing physical source {}, associated logical source \"{}\" was not associated with it anymore",
        physicalSource.getPhysicalSourceId(),
        logicalSourceIter->second.getLogicalSourceName());

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
