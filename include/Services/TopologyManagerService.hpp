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

#ifndef NES_SRC_SERVICES_TOPOLOGYMANAGERSERVICE_H_
#define NES_SRC_SERVICES_TOPOLOGYMANAGERSERVICE_H_

#include <Services/StreamCatalogService.hpp>
#include <atomic>
#include <memory>
#include <mutex>

enum NodeType : int;
namespace NES {
class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

/**
 * @brief: This class is responsible for registering/unregistering nodes and adding and removing parentNodes.
 */
class TopologyManagerService {

  public:
    TopologyManagerService(TopologyPtr topology, StreamCatalogPtr streamCatalog);

    /**
     * @brief registers a node
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
                          NodeType type);

    /**
    * @brief unregister an existing node
    * @param nodeId
    * @return bool indicating success
    */
    bool unregisterNode(uint64_t nodeId);

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

    /**
     * @brief calls the method findNodeWithId of Topology class
     * @param nodeId
     * @return TopologyNodePtr
     */
    TopologyNodePtr findNodeWithId(uint64_t nodeId);

  private:
    TopologyPtr topology;
    StreamCatalogPtr streamCatalog;
    std::mutex registerDeregisterNode;
    std::atomic_uint64_t topologyNodeIdCounter = 0;

    /**
     * @brief method to generate the next (monotonically increasing) topology node id
     * @return next topology node id
     */
    uint64_t getNextTopologyNodeId();
};

using TopologyManagerServicePtr = std::shared_ptr<TopologyManagerService>;

}//namespace NES

#endif//NES_SRC_SERVICES_TOPOLOGYMANAGERSERVICE_H_
