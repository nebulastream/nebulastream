#ifndef NES_NETWORKCOMMON_HPP
#define NES_NETWORKCOMMON_HPP

#include <cstdint>
#include <string>
#include <zmq.hpp>
#include <tuple>

namespace NES {
namespace Network {

static constexpr uint16_t DEFAULT_NUM_SERVER_THREADS = 3;

using NodeId = uint64_t;
using SubpartitionId = uint64_t;
using PartitionId = uint64_t;
using OperatorId = uint64_t;
using QueryId = uint64_t;

class NesPartition {
  public:
    explicit NesPartition(QueryId queryId,
                          OperatorId operatorId,
                          PartitionId partitionId,
                          SubpartitionId subpartitionId) : queryId(queryId),
                                                           operatorId(operatorId),
                                                           partitionId(partitionId),
                                                           subpartitionId(subpartitionId) {}
    /**
     * @brief getter for the queryId
     * @return the queryId
     */
    QueryId getQueryId() const {
        return queryId;
    }

    /**
     * @brief getter for the operatorId
     * @return the operatorId
     */
    OperatorId getOperatorId() const {
        return operatorId;
    }

    /**
     * @brief getter for the partitionId
     * @return the partitionId
     */
    PartitionId getPartitionId() const {
        return partitionId;
    }

    /**
     * @brief getter for the getSubpartitionId
     * @return the subpartitionId
     */
    SubpartitionId getSubpartitionId() const {
        return subpartitionId;
    }

    const std::string toString() const {
        return std::to_string(queryId) + "::" + std::to_string(operatorId) + "::"
            + std::to_string(partitionId) + "::" + std::to_string(subpartitionId);
    }

    friend bool operator==(const NesPartition& lhs, const NesPartition& rhs) {
        return lhs.queryId == rhs.queryId &&
            lhs.operatorId == rhs.operatorId &&
            lhs.partitionId == rhs.partitionId &&
            lhs.subpartitionId == rhs.subpartitionId;
    }

  private:
    const QueryId queryId;
    const OperatorId operatorId;
    const PartitionId partitionId;
    const SubpartitionId subpartitionId;
};

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
}// namespace Network
}// namespace NES

// this is required to add the hashing function for NesPartition to std namespace
namespace std {
template<>
struct hash<NES::Network::NesPartition> {
    std::size_t operator()(const NES::Network::NesPartition& k) const {
        using std::hash;

        // Hash function for the NesPartition
        // Compute individual hash values of the Ints and combine them using XOR and bit shifting:
        return ((hash<uint64_t>()(k.getQueryId())
            ^ (hash<uint64_t>()(k.getOperatorId()) << 1)) >> 1)
            ^ ((hash<uint64_t>()(k.getPartitionId())
                ^ (hash<uint64_t>()(k.getSubpartitionId()) << 1)) >> 1);
    }
};

}

#endif//NES_NETWORKCOMMON_HPP
