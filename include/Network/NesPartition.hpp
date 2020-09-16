#ifndef NES_INCLUDE_NETWORK_NESPARTITION_HPP_
#define NES_INCLUDE_NETWORK_NESPARTITION_HPP_
#include <cstdint>
#include <string>

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
    explicit NesPartition(QueryId queryId, OperatorId operatorId, PartitionId partitionId,
                          SubpartitionId subpartitionId)
        : queryId(queryId), operatorId(operatorId), partitionId(partitionId), subpartitionId(subpartitionId) {
    }

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

    std::string toString() const {
        return std::to_string(queryId) + "::" + std::to_string(operatorId) + "::"
            + std::to_string(partitionId) + "::" + std::to_string(subpartitionId);
    }

    friend std::ostream& operator<<(std::ostream& os, const NesPartition& partition) {
        os << partition.toString();
        return os;
    }

    /**
     * @brief The equals operator for the NesPartition. It is not comparing threadIds
     * @param lhs
     * @param rhs
     * @return
     */
    friend bool operator==(const NesPartition& lhs, const NesPartition& rhs) {
        return lhs.queryId == rhs.queryId && lhs.operatorId == rhs.operatorId && lhs.partitionId == rhs.partitionId
            && lhs.subpartitionId == rhs.subpartitionId;
    }

  private:
    const QueryId queryId;
    const OperatorId operatorId;
    const PartitionId partitionId;
    const SubpartitionId subpartitionId;
};
}// namespace Network
}// namespace NES
namespace std {
template<>
struct hash<NES::Network::NesPartition> {
    std::size_t operator()(const NES::Network::NesPartition& k) const {
        using std::hash;

        // Hash function for the NesPartition
        // Compute individual hash values of the Ints and combine them using XOR and bit shifting:
        return ((hash<uint64_t>()(k.getQueryId())
                 ^ (hash<uint64_t>()(k.getOperatorId()) << 1))
                >> 1)
            ^ ((hash<uint64_t>()(k.getPartitionId())
                ^ (hash<uint64_t>()(k.getSubpartitionId()) << 1))
               >> 1);
    }
};

}// namespace std
#endif//NES_INCLUDE_NETWORK_NESPARTITION_HPP_
