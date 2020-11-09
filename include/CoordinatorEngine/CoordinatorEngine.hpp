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

#ifndef NES_INCLUDE_COORDINATORENGINE_COORDINATORENGINE_HPP_
#define NES_INCLUDE_COORDINATORENGINE_COORDINATORENGINE_HPP_
#include <CoordinatorRPCService.pb.h>
#include <NodeStats.pb.h>

#include <memory>
#include <mutex>

namespace NES {
class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

class CoordinatorEngine {

  public:
    CoordinatorEngine(StreamCatalogPtr streamCatalog, TopologyPtr topology);
    ~CoordinatorEngine();
    /**
     * @brief registers a node
     * @param address of node ip:port
     * @param cpu the cpu capacity of the worker
     * @param nodeProperties of the to be added sensor
     * @param node type
     * @return id of node
     */
    size_t registerNode(std::string address, int64_t grpcPort, int64_t dataPort, uint16_t numberOfSlots, NodeStats nodeStats, NodeType type);

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
    bool registerPhysicalStream(uint64_t nodeId,
                                std::string sourceType,
                                std::string sourceConf,
                                size_t sourceFrequency,
                                size_t numberOfTuplesToProducePerBuffer,
                                size_t numberOfBuffersToProduce,
                                std::string physicalStreamname,
                                std::string logicalStreamname);

    /**
     * @brief method to unregister a physical stream
     * @param nodeId
     * @param logicalStreamName
     * @param physicalStreamName
     * @return bool indicating success
     */
    bool unregisterPhysicalStream(uint64_t nodeId, std::string physicalStreamName, std::string logicalStreamName);

    /**
     * @brief method to register a logical stream
     * @param logicalStreamName
     * @param schemaString
     * @return bool indicating success
     */
    bool registerLogicalStream(std::string logicalStreamName, std::string schemaString);

    /**
     * @brief method to unregister a logical stream
     * @param logicalStreamName
     * @return bool indicating success
     */
    bool unregisterLogicalStream(std::string logicalStreamName);

    /**
     * @brief method to ad a new parent to a node
     * @param childId
     * @param parentId
     * @return bool indicating success
     */
    bool addParent(uint64_t childId, uint64_t parentId);

    /**
     * @brief method to remove an existing parent from a node
     * @param childId
     * @param parentId
     * @return bool indicating success
     */
    bool removeParent(uint64_t childId, uint64_t parentId);

  private:
    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
    std::mutex registerDeregisterNode;
    std::mutex addRemoveLogicalStream;
    std::mutex addRemovePhysicalStream;
};

typedef std::shared_ptr<CoordinatorEngine> CoordinatorEnginePtr;

}// namespace NES

#endif//NES_INCLUDE_COORDINATORENGINE_COORDINATORENGINE_HPP_
