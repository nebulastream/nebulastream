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

#ifndef NES_INCLUDE_NETWORK_NODELOCATION_HPP_
#define NES_INCLUDE_NETWORK_NODELOCATION_HPP_

#include <Network/NesPartition.hpp>
#include <Util/Logger.hpp>

namespace NES::Network {

/**
 * @brief This is a network location of a nes node. It contains a node id, an ip address, and port for data transfer.
 */
class NodeLocation {
  public:
    explicit NodeLocation(NodeId nodeId, std::string hostname, uint32_t port)
        : nodeId(nodeId), hostname(std::move(hostname)), port(port) {
        NES_ASSERT2_FMT(this->hostname.size() > 0, "Empty hostname passed on " << nodeId);
    }

    NodeLocation(const NodeLocation& other) : nodeId(other.nodeId), hostname(other.hostname), port(other.port) {}

    NodeLocation& operator=(const NodeLocation& other) {
        nodeId = other.nodeId;
        hostname = other.hostname;
        port = other.port;
        return *this;
    }

    /**
     * @brief Returns the zmq uri for connection
     * @return the zmq uri for connection
     */
    [[nodiscard]] std::string createZmqURI() const { return "tcp://" + hostname + ":" + std::to_string(port); }

    /**
     * @brief Return the node id
     * @return the node id
     */
    [[nodiscard]] NodeId getNodeId() const { return nodeId; }

    /**
     * @brief Returns the hostname
     * @return the hostname
     */
    [[nodiscard]] const std::string& getHostname() const { return hostname; }

    /**
     * @brief Returns the port
     * @return the port
     */
    [[nodiscard]] uint32_t getPort() const { return port; }

    /**
     * @brief The equals operator for the NodeLocation.
     * @param lhs left node location
     * @param rhs right node location
     * @return true, if they are equal, else false
     */
    friend bool operator==(const NodeLocation& lhs, const NodeLocation& rhs) {
        return lhs.nodeId == rhs.nodeId && lhs.hostname == rhs.hostname && lhs.port == rhs.port;
    }

  private:
    NodeId nodeId;
    std::string hostname;
    uint32_t port;
};
}// namespace NES::Network
#endif  // NES_INCLUDE_NETWORK_NODELOCATION_HPP_
