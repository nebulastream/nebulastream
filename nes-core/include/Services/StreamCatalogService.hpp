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

#ifndef NES_INCLUDE_SERVICES_STREAMCATALOGSERVICE_HPP_
#define NES_INCLUDE_SERVICES_STREAMCATALOGSERVICE_HPP_

#include <memory>
#include <mutex>

namespace NES {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

/**
 * @brief: This class is responsible for registering/unregistering physical and logical streams.
 */
class StreamCatalogService {

  public:
    StreamCatalogService(SourceCatalogPtr streamCatalog);

    /**
     * @brief method to register a physical stream
     * @param topologyNode : the topology node
     * @param logicalSourceName: logical source name
     * @param physicalSourceName: physical source name
     * @return bool indicating success
     */
    bool registerPhysicalStream(TopologyNodePtr topologyNode,
                                const std::string& physicalSourceName,
                                const std::string& logicalSourceName);

    /**
     * @brief method to unregister a physical stream
     * @param topologyNode : the topology node
     * @param logicalSourceName: logical source name
     * @param physicalSourceName: physical source name
     * @return bool indicating success
     */
    bool unregisterPhysicalStream(TopologyNodePtr topologyNode,
                                  const std::string& physicalSourceName,
                                  const std::string& logicalSourceName);

    /**
     * @brief method to register a logical stream
     * @param logicalSourceName: name of the logical source
     * @param schemaString: schema as string
     * @return bool indicating success
     */
    bool registerLogicalSource(const std::string& logicalSourceName, const std::string& schemaString);

    /**
     * @brief method to register a logical stream
     * @param logicalSourceName: logical source name
     * @param schema: schema object
     * @return bool indicating success
     */
    bool registerLogicalSource(const std::string& logicalSourceName, SchemaPtr schema);

    /**
     * @brief method to unregister a logical stream
     * @param logicalStreamName
     * @return bool indicating success
     */
    bool unregisterLogicalStream(const std::string& logicalStreamName);

  private:
    SourceCatalogPtr streamCatalog;
    std::mutex addRemoveLogicalStream;
    std::mutex addRemovePhysicalStream;
};
using StreamCatalogServicePtr = std::shared_ptr<StreamCatalogService>;
}// namespace NES
#endif// NES_INCLUDE_SERVICES_STREAMCATALOGSERVICE_HPP_
