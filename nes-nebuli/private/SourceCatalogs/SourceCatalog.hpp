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
#include <vector>
#include <API/Schema.hpp>
#include <SourceCatalogs/SourceCatalogEntry.hpp>

namespace NES
{


namespace Catalogs::Source
{
/// @brief the source catalog handles the mapping of logical to physical sources
class SourceCatalog final
{
public:
    SourceCatalog() = default;
    ~SourceCatalog() = default;

    /// @brief method to add a logical source
    /// @param logicalSourceName logical source name
    /// @param schema of logical source as object
    /// @return bool indicating if insert was successful
    bool addLogicalSource(const std::string& logicalSourceName, Schema schema);

    /// @brief method to delete a logical source
    /// @caution this method only remove the entry from the catalog not from the topology
    /// @param logicalSourceName name of logical source to delete
    /// @return bool indicating the success of the removal
    bool removeLogicalSource(const std::string& logicalSourceName);

    /// @brief method to add a physical source
    /// @caution combination of node and name has to be unique
    /// @param logicalSourceName logical source name
    /// @param sourceCatalogEntry the source catalog entry to store
    /// @return bool indicating success of insert source
    bool addPhysicalSource(const std::string& logicalSourceName, const std::shared_ptr<SourceCatalogEntry>& sourceCatalogEntry);

    /// @brief method to remove a physical source
    /// @param logicalSourceName name of the logical source
    /// @param physicalSourceName name of the physical source
    /// @param topologyNodeId id of the topology node
    /// @return bool indicating success of remove source
    bool removePhysicalSource(const std::string& logicalSourceName, const std::string& physicalSourceName, WorkerId topologyNodeId);

    /// @brief method to remove all physical sources of a single worker
    /// @param topologyNodeId worker node identifier
    /// @return number of sucessfully removed physical sources
    size_t removeAllPhysicalSourcesByWorker(WorkerId topologyNodeId);

    /// @brief method to get the schema from the given logical source
    /// @param logicalSourceName name of the logical source name
    /// @return the pointer to the schema
    Schema getSchemaForLogicalSource(const std::string& logicalSourceName);

    /// @brief method to return the logical source for an existing logical source
    /// @param logicalSourceName name of the logical source
    /// @return smart pointer to the logical source else nullptr
    /// @note the source will also contain the schema
    std::shared_ptr<LogicalSource> getLogicalSource(const std::string& logicalSourceName);

    /// @brief method to return the source for an existing logical source or throw exception
    /// @param logicalSourceName name of logical source
    /// @return smart pointer to a physical source else nullptr
    /// @note the source will also contain the schema
    /// @throws UnknownSourceType
    std::shared_ptr<LogicalSource> getLogicalSourceOrThrowException(const std::string& logicalSourceName);

    /// @brief Check if logical source with this name exists
    /// @param logicalSourceName name of the logical source
    /// @return bool indicating if source exists
    bool containsLogicalSource(const std::string& logicalSourceName);

    /// @brief return all topology node ids that host physical sources that contribute to this logical source
    /// @param logicalSourceName name of logical source
    /// @return list of topology nodes ids
    std::vector<WorkerId> getSourceNodesForLogicalSource(const std::string& logicalSourceName);

    /// @brief Return a list of logical source names registered at catalog
    /// @return map containing source name as key and schema object as value
    std::map<std::string, Schema> getAllLogicalSource();

    /// @brief Get all logical sources with their schema as string
    /// @return map of logical source name to schema as string
    std::map<std::string, std::string> getAllLogicalSourceAsString();

    /// @brief method to return the physical source and the associated schemas
    /// @return string containing the content of the catalog
    std::string getPhysicalSourceAndSchemaAsString();

    /// @brief get all physical sources for a logical source
    /// @param logicalSourceName name of the logical source
    /// @return vector containing source catalog entries
    std::vector<std::shared_ptr<SourceCatalogEntry>> getPhysicalSources(const std::string& logicalSourceName);

    /// @brief method to update a logical source schema
    /// @param logicalSourceName logical source name
    /// @param schema of logical source as object
    /// @return bool indicating if update was successful
    bool updateLogicalSource(const std::string& logicalSourceName, std::shared_ptr<Schema> schema);

private:
    /// @brief test if logical source with this name exists
    /// @param logicalSourceName name of the logical source to test
    /// @return bool indicating if source exists
    bool testIfLogicalSourceExistsInLogicalToPhysicalMapping(const std::string& logicalSourceName);

    std::recursive_mutex catalogMutex;
    /// map logical source to schema
    std::map<std::string, Schema> logicalSourceNameToSchemaMapping;
    /// map logical source to physical source
    std::map<std::string, std::vector<std::shared_ptr<SourceCatalogEntry>>> logicalToPhysicalSourceMapping;
};
}
}
