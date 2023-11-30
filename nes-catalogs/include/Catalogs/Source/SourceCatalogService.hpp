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

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_SOURCE_SOURCECATALOGSERVICE_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_SOURCE_SOURCECATALOGSERVICE_HPP_

#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace NES {
class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class LogicalSource;
using LogicalSourcePtr = std::shared_ptr<LogicalSource>;

namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;

class SourceCatalogEntry;
using SourceCatalogEntryPtr = std::shared_ptr<SourceCatalogEntry>;
}// namespace Catalogs::Source

/**
 * @brief: This class is responsible for registering/unregistering physical and logical sources.
 */
class SourceCatalogService {

  public:
    SourceCatalogService(Catalogs::Source::SourceCatalogPtr sourceCatalog);

    /**
     * @brief method to register a physical source
     * @param topologyNode : the topology node
     * @param logicalSourceName: logical source name
     * @param physicalSourceName: physical source name
     * @return bool indicating success
     */
    bool registerPhysicalSource(TopologyNodePtr topologyNode,
                                const std::string& physicalSourceName,
                                const std::string& logicalSourceName);

    /**
     * @brief method to unregister a physical source
     * @param topologyNode : the topology node
     * @param logicalSourceName: logical source name
     * @param physicalSourceName: physical source name
     * @return bool indicating success
     */
    bool unregisterPhysicalSource(TopologyNodePtr topologyNode,
                                  const std::string& physicalSourceName,
                                  const std::string& logicalSourceName);

    /**
     * @brief method to register a logical source
     * @param logicalSourceName: logical source name
     * @param schema: schema object
     * @return bool indicating success
     */
    bool registerLogicalSource(const std::string& logicalSourceName, SchemaPtr schema);

    /**
     * @brief method to update schema of an existing logical source
     * @param logicalSourceName: logical source name
     * @param schema: schema object
     * @return bool indicating success
     */
    bool updateLogicalSource(const std::string& logicalSourceName, SchemaPtr schema);

    /**
     * @brief method to unregister a logical source
     * @param logicalSourceName
     * @return bool indicating success
     */
    bool unregisterLogicalSource(const std::string& logicalSourceName);

    /**
     * @brief method returns a copy of logical source
     * @param logicalSourceName: name of the logical source
     * @return copy of the logical source
     */
    LogicalSourcePtr getLogicalSource(const std::string& logicalSourceName);

    /**
     * @brief return all logical sources as string
     * @return map of logical source name and string representing logical source information
     */
    std::map<std::string, std::string> getAllLogicalSourceAsString();

    /**
     * Get information about all physical sources
     * @param logicalSourceName: logical source name
     * @return vector containing source catalog entry
     */
    std::vector<Catalogs::Source::SourceCatalogEntryPtr> getPhysicalSources(const std::string& logicalSourceName);

    /**
     * @brief Reset source catalog by clearing it from all physical and logical sources
     * @return true if successful
     */
    bool reset();

  private:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::mutex addRemoveLogicalSource;
    std::mutex addRemovePhysicalSource;
};
using SourceCatalogServicePtr = std::shared_ptr<SourceCatalogService>;
}// namespace NES
#endif  // NES_CATALOGS_INCLUDE_CATALOGS_SOURCE_SOURCECATALOGSERVICE_HPP_
