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

namespace NES {
namespace Network {

class NodeLocation {
  public:
    explicit NodeLocation(NodeId nodeId, const std::string& hostname, uint32_t port)
        : nodeId(nodeId), hostname(hostname), port(port) {
    }

    std::string createZmqURI() const {
        return "tcp://" + hostname + ":" + std::to_string(port);
    }

    NodeId getNodeId() const {
        return nodeId;
    }

    const std::string& getHostname() const {
        return hostname;
    }

    uint32_t getPort() const {
        return port;
    }

    /**
     * @brief The equals operator for the NodeLocation.
     * @param lhs
     * @param rhs
     * @return true, if they are equal, else false
     */
    friend bool operator==(const NodeLocation& lhs, const NodeLocation& rhs) {
        return lhs.nodeId == rhs.nodeId && lhs.hostname == rhs.hostname && lhs.port == rhs.port;
    }

  private:
    const NodeId nodeId;
    const std::string hostname;
    const uint32_t port;
};
}// namespace Network
}// namespace NES
#endif//NES_INCLUDE_NETWORK_NODELOCATION_HPP_
