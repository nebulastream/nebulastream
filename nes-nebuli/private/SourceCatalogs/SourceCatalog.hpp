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

#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <SourceCatalogs/SourceCatalogEntry.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{


namespace Catalogs::Source
{
/// @brief the source catalog handles the mapping of logical to physical sources
class SourceCatalog
{
public:
    SourceCatalog() = default;
    ~SourceCatalog() = default;

    /// @brief method to add a logical source
    /// @param logicalSourceName logical source name
    /// @param schema of logical source as object
    /// @return bool indicating if insert was successful
    std::optional<LogicalSource> addLogicalSource(const std::string& logicalSourceName, const Schema& schema);


    /// @brief method to delete a logical source
    /// @caution this method only remove the entry from the catalog not from the topology
    /// @param logicalSourceName name of logical source to delete
    /// @return bool indicating the success of the removal
    bool removeLogicalSource(const std::string& logicalSourceName);

    Sources::SourceDescriptor createPhysicalSource(
        Configurations::DescriptorConfig::Config&& descriptorConfig,
        const LogicalSource& logicalSource,
        WorkerId workerId,
        std::string sourceType,
        const Sources::ParserConfig& parserConfig);

    ///returns true if there is a source descriptor with that id registered and it was removed
    bool removePhysicalSource(const Sources::SourceDescriptor& physicalSource);
    // bool associate(const LogicalSource& logicalSource, const PhysicalSource& physicalSource);
    //
    //
    // bool dissociate(const LogicalSource& logicalSource, const PhysicalSource& physicalSource);

    [[nodiscard]] std::optional<LogicalSource> getLogicalSource(const std::string& logicalSourceName) const;

    [[nodiscard]] bool containsLogicalSource(const LogicalSource& logicalSource) const;
    /// @brief Check if logical source with this name exists
    /// @param logicalSourceName name of the logical source
    /// @return bool indicating if source exists
    [[nodiscard]] bool containsLogicalSource(const std::string& logicalSourceName) const;

    [[nodiscard]] Sources::SourceDescriptor getPhysicalSource(uint64_t physicalSourceID) const;


    [[nodiscard]] std::set<Sources::SourceDescriptor> getPhysicalSources(const LogicalSource& logicalSource) const;
    /// @brief get all physical sources for a logical source
    /// @param logicalSourceName name of the logical source
    /// @return vector containing source catalog entries
    [[nodiscard]] std::optional<std::set<Sources::SourceDescriptor>> getPhysicalSources(const std::string& logicalSourceName) const;

    [[nodiscard]] std::unordered_set<LogicalSource> getAllLogicalSources() const;
    [[nodiscard]] std::unordered_map<LogicalSource, std::set<Sources::SourceDescriptor>> getAllSources() const;


private:
    std::recursive_mutex catalogMutex;
    /// map logical source to schema
    std::unordered_map<std::string, LogicalSource> namesToLogicalSourceMapping{};
    std::map<uint64_t, Sources::SourceDescriptor> idsToPhysicalSources{};
    /// map logical source to physical source
    std::unordered_map<LogicalSource, std::set<Sources::SourceDescriptor>> logicalToPhysicalSourceMapping{};
    // std::unordered_map<PhysicalSource, std::unordered_set<LogicalSource>> physicalToLogicalSourceMappings{};
};
}
}
