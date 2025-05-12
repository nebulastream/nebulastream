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

#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <API/Schema.hpp>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>


namespace NES::Catalogs::Source
{
/// @brief the source catalog handles the mapping of logical to physical sources
class SourceCatalog
{
public:
    SourceCatalog() = default;
    ~SourceCatalog() = default;

    SourceCatalog(const SourceCatalog&) = delete;
    SourceCatalog(SourceCatalog&&) = delete;
    SourceCatalog& operator=(const SourceCatalog&) = delete;
    SourceCatalog& operator=(SourceCatalog&&) = delete;

    /// @brief method to add a logical source
    /// @param logicalSourceName logical source name
    /// @param schema of logical source as object
    /// @return the created logical source if successful, nullopt if a logical source with that name already existed
    std::optional<LogicalSource> addLogicalSource(const std::string& logicalSourceName, const Schema& schema);


    /// @brief method to delete a logical source and any associated physical source.
    /// @caution this method only remove the entry from the catalog not from the topology.
    /// @param logicalSourceName name of logical source to delete
    /// @return bool indicating if this logical source was registered by name and removed
    bool removeLogicalSource(const LogicalSource& logicalSource);

    /// @brief creates a new physical source and associates it with a logical source
    /// @return nullopt if the logical source is not registered anymore, else a source descriptor with an assigned id
    [[nodiscard]] std::optional<Sources::SourceDescriptor> addPhysicalSource(
        const LogicalSource& logicalSource,
        WorkerId workerId,
        std::string sourceType,
        int buffersInLocalPool,
        Configurations::DescriptorConfig::Config&& descriptorConfig,
        const Sources::ParserConfig& parserConfig);

    /// @brief removes a physical source
    /// @return true if there is a source descriptor with that id registered and it was removed
    bool removePhysicalSource(const Sources::SourceDescriptor& physicalSource);

    [[nodiscard]] std::optional<LogicalSource> getLogicalSource(const std::string& logicalSourceName) const;

    [[nodiscard]] bool containsLogicalSource(const LogicalSource& logicalSource) const;
    [[nodiscard]] bool containsLogicalSource(const std::string& logicalSourceName) const;

    [[nodiscard]] std::optional<Sources::SourceDescriptor> getPhysicalSource(uint64_t physicalSourceID) const;

    /// @brief retrieves physical sources for a logical source
    /// @returns nullopt if the logical source is not registered anymore, else the set of source descriptors associated with it
    [[nodiscard]] std::optional<std::unordered_set<Sources::SourceDescriptor>> getPhysicalSources(const LogicalSource& logicalSource) const;

    [[nodiscard]] std::unordered_set<LogicalSource> getAllLogicalSources() const;
    [[nodiscard]] std::unordered_map<LogicalSource, std::unordered_set<Sources::SourceDescriptor>> getAllSources() const;


private:
    mutable std::recursive_mutex catalogMutex;
    std::atomic_uint64_t nextPhysicaSourceID{0};
    /// map logical source to schema
    std::unordered_map<std::string, LogicalSource> namesToLogicalSourceMapping;
    std::unordered_map<uint64_t, Sources::SourceDescriptor> idsToPhysicalSources;
    /// map logical source to physical source
    std::unordered_map<LogicalSource, std::unordered_set<Sources::SourceDescriptor>> logicalToPhysicalSourceMapping;
};
}
