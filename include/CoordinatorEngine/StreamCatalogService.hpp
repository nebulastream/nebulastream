/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_SRC_COORDINATORENGINE_STREAMCATALOGSERVICE_H_
#define NES_SRC_COORDINATORENGINE_STREAMCATALOGSERVICE_H_

#include <memory>
#include <mutex>

#include <CoordinatorEngine/TopologyManagerService.hpp>

enum NodeType : int;
namespace NES {
class StreamCatalog;
using StreamCatalogPtr = std::shared_ptr<StreamCatalog>;
class Topology;
using TopologyPtr = std::shared_ptr<Topology>;
class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;
class NodeStats;
using NodeStatsPtr = std::shared_ptr<NodeStats>;
class TopologyManagerService;

class StreamCatalogService {

  public:
    StreamCatalogService(StreamCatalogPtr streamCatalog);
    ~StreamCatalogService();
    /**
     * @brief registers a node in the StreamCatalog
     * @param address of node ip:port
     * @param cpu the cpu capacity of the worker
     * @param nodeProperties of the to be added sensor
     * @param node type
     * @return id of node
     */
    uint64_t registerNode(const std::string& address,
                          int64_t grpcPort,
                          int64_t dataPort,
                          uint16_t numberOfSlots,
                          const NodeStatsPtr& nodeStats);

    /**
     * @brief unregister an existing node
     * @param nodeId
     * @return bool indicating success
     */
    bool unregisterNode(uint64_t nodeId);

    /**
     * @brief method to register a physical stream
     * @param nodeId
     * @param sourcetype
     * @param sourceconf
     * @param sourcefrequency
     * @param numberofbufferstoproduce
     * @param physicalstreamname
     * @param logicalstreamname
     * @return bool indicating success
     */
    bool registerPhysicalStream(TopologyNodePtr physicalNode,
                                const std::string& sourceType,
                                const std::string& physicalStreamname,
                                const std::string& logicalStreamname);

    /**
     * @brief method to unregister a physical stream
     * @param nodeId
     * @param logicalStreamName
     * @param physicalStreamName
     * @return bool indicating success
     */
    bool unregisterPhysicalStream(TopologyNodePtr physicalNode,
                                  const std::string& physicalStreamName,
                                  const std::string& logicalStreamName);


    /**
     * @brief method to register a logical stream
     * @param logicalStreamName
     * @param schemaString
     * @return bool indicating success
     */
    bool registerLogicalStream(const std::string& logicalStreamName, const std::string& schemaString);

    /**
     * @brief method to unregister a logical stream
     * @param logicalStreamName
     * @return bool indicating success
     */
    bool unregisterLogicalStream(const std::string& logicalStreamName);


  private:
    StreamCatalogPtr streamCatalog;
    std::mutex registerDeregisterNode;
    std::mutex addRemoveLogicalStream;
    std::mutex addRemovePhysicalStream;
};

}// namespace NES
#endif//NES_SRC_COORDINATORENGINE_STREAMCATALOGSERVICE_H_
