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
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>


namespace NES
{
/// @brief The source catalog handles the mapping of logical to physical sources.
/// We expect the class to be used behind frontends that permit concurrent read-write access (like a REST server),
/// so all individual operations in this class are thread safe and atomic.
class SourceCatalog
{
public:
    SourceCatalog() = default;
    ~SourceCatalog() = default;

    SourceCatalog(const SourceCatalog&) = delete;
    SourceCatalog(SourceCatalog&&) = delete;
    SourceCatalog& operator=(const SourceCatalog&) = delete;
    SourceCatalog& operator=(SourceCatalog&&) = delete;

    /// @param schema the schema of fields without the logical source name as a prefix
    /// @return the created logical source if successful with a schema containing the logical source name as a prefix,
    /// nullopt if a logical source with that name already existed
    [[nodiscard]] std::optional<LogicalSource> addLogicalSource(const std::string& logicalSourceName, const Schema& schema);


    /// @brief method to delete a logical source and any associated physical source.
    /// @return bool indicating if this logical source was registered by name and removed
    [[nodiscard]] bool removeLogicalSource(const LogicalSource& logicalSource);

    /// @brief creates a new physical source and associates it with a logical source
    /// @return nullopt if the logical source is not registered anymore, otherwise a source descriptor with an assigned id
    [[nodiscard]] std::optional<SourceDescriptor> addPhysicalSource(
        const LogicalSource& logicalSource,
        const std::string& workerId,
        const std::string& sourceType,
        int buffersInLocalPool,
        NES::Configurations::DescriptorConfig::Config&& descriptorConfig,
        const ParserConfig& parserConfig);

    /// @brief removes a physical source
    /// @return true if there is a source descriptor with that id registered and it was removed
    [[nodiscard]] bool removePhysicalSource(const SourceDescriptor& physicalSource);

    [[nodiscard]] std::optional<LogicalSource> getLogicalSource(const std::string& logicalSourceName) const;

    [[nodiscard]] bool containsLogicalSource(const LogicalSource& logicalSource) const;
    [[nodiscard]] bool containsLogicalSource(const std::string& logicalSourceName) const;

    [[nodiscard]] std::optional<SourceDescriptor> getPhysicalSource(uint64_t physicalSourceId) const;

    /// @brief retrieves physical sources for a logical source
    /// @returns nullopt if the logical source is not registered anymore, else the set of source descriptors associated with it
    [[nodiscard]] std::optional<std::unordered_set<SourceDescriptor>> getPhysicalSources(const LogicalSource& logicalSource) const;

    [[nodiscard]] std::unordered_set<LogicalSource> getAllLogicalSources() const;
    [[nodiscard]] std::unordered_map<LogicalSource, std::unordered_set<SourceDescriptor>> getLogicalToPhysicalSourceMapping() const;


private:
    mutable std::recursive_mutex catalogMutex;
    std::atomic_uint64_t nextPhysicalSourceId{0};
    std::unordered_map<std::string, LogicalSource> namesToLogicalSourceMapping;
    std::unordered_map<uint64_t, SourceDescriptor> idsToPhysicalSources;
    std::unordered_map<LogicalSource, std::unordered_set<SourceDescriptor>> logicalToPhysicalSourceMapping;
};
}
