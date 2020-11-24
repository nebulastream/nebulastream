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

#ifndef NES_TOPOLOGYNODE_HPP
#define NES_TOPOLOGYNODE_HPP

#include <NodeStats.pb.h>
#include <Nodes/Node.hpp>

namespace NES {

class TopologyNode;
typedef std::shared_ptr<TopologyNode> TopologyNodePtr;

/**
 * @brief This class represents information about a physical node participating in the NES infrastructure
 */
class TopologyNode : public Node {

  public:
    static TopologyNodePtr create(uint64_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resources);

    /**
     * @brief method to get the id of the node
     * @return id as a uint64_t
     */
    uint64_t getId();

    /**
     * @brief method to get the overall cpu capacity of the node
     * @return uint64_t cpu capacity
     */
    uint16_t getAvailableResources();

    /**
     * @brief method to reduce the cpu capacity of the node
     * @param uint64_t of the value that has to be subtracted
     */
    void reduceResources(uint16_t usedCapacity);

    /**
     * @brief method to increase CPU capacity
     * @param uint64_t of the vlaue that has to be added
     */
    void increaseResources(uint16_t freedCapacity);

    /**
     * @brief method to set the property of the node by creating a NodeProperties object
     * @param a string of the serialized json object of the properties
     */
    void setNodeStats(NodeStats nodeStats);

    /**
   * @brief method to get the node properties
   * @NodePropertiesPtr to the properties of this node
   */
    NodeStats getNodeStats();

    /**
     * @brief Get ip address of the node
     * @return ip address
     */
    const std::string getIpAddress() const;

    /**
     * @brief Get grpc port for the node
     * @return grpc port
     */
    uint32_t getGrpcPort() const;

    /**
     * @brief Get the data port for the node
     * @return data port
     */
    uint32_t getDataPort() const;

    const std::string toString() const override;

    /**
     * @brief Create a shallow copy of the physical node i.e. without copying the parent and child nodes
     * @return
     */
    TopologyNodePtr copy();

    explicit TopologyNode(uint64_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resources);

    bool containAsParent(NodePtr node) override;

    bool containAsChild(NodePtr ptr) override;

  private:
    uint64_t id;
    std::string ipAddress;
    uint32_t grpcPort;
    uint32_t dataPort;
    uint16_t resources;
    uint16_t usedResources;
    NodeStats nodeStats;

    //FIXME: as part of the issue #955 A potential solution for adding network link properties
    //    std::vector<LinkPropertiesPtr> inputLinks;
    //    std::vector<LinkPropertiesPtr> outputLinks;
};
}// namespace NES

#endif//NES_TOPOLOGYNODE_HPP
