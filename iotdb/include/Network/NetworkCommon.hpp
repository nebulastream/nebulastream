#ifndef NES_NETWORKCOMMON_HPP
#define NES_NETWORKCOMMON_HPP

#include <cstdint>
#include <string>
#include <zmq.hpp>

namespace NES {
namespace Network {

static constexpr uint16_t DEFAULT_NUM_SERVER_THREADS = 3;

using NodeId = uint32_t;
using SubpartitionId = uint32_t;
using PartitionId = uint32_t;
using OperatorId = uint32_t;
using QueryId = uint32_t;

class NodeLocation {
  private:
    const NodeId nodeId;
    const std::string hostname;
    const uint16_t port;
  public:
    explicit NodeLocation(NodeId nodeId, const std::string& hostname, uint16_t port)
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

    const uint16_t getPort() const {
        return port;
    }
};
}
}

#endif //NES_NETWORKCOMMON_HPP
