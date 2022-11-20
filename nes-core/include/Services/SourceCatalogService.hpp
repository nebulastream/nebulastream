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

#ifndef NES_CORE_INCLUDE_SERVICES_SOURCECATALOGSERVICE_HPP_
#define NES_CORE_INCLUDE_SERVICES_SOURCECATALOGSERVICE_HPP_

#include <memory>
#include <mutex>

namespace NES {
class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
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
     * @param logicalSourceName: name of the logical source
     * @param schemaString: schema as string
     * @return bool indicating success
     */
    bool registerLogicalSource(const std::string& logicalSourceName, const std::string& schemaString);

    /**
     * @brief method to register a logical source
     * @param logicalSourceName: logical source name
     * @param schema: schema object
     * @return bool indicating success
     */
    bool registerLogicalSource(const std::string& logicalSourceName, SchemaPtr schema);

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

  private:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::mutex addRemoveLogicalSource;
    std::mutex addRemovePhysicalSource;
};
using SourceCatalogServicePtr = std::shared_ptr<SourceCatalogService>;
}// namespace NES
#endif// NES_CORE_INCLUDE_SERVICES_SOURCECATALOGSERVICE_HPP_
