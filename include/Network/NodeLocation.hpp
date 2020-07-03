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
}
}
#endif//NES_INCLUDE_NETWORK_NODELOCATION_HPP_
