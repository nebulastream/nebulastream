/*
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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_NETWORK_NESPARTITION_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_NETWORK_NESPARTITION_HPP_

#include <Identifiers.hpp>
#include <cstdint>
#include <string>
#include <fmt/core.h>

namespace NES::Network {
static constexpr uint16_t DEFAULT_NUM_SERVER_THREADS = 3;

class NesPartition {
  public:
    explicit NesPartition(QueryId queryId, OperatorId operatorId, PartitionId partitionId, SubpartitionId subpartitionId);

    /**
     * @brief getter for the queryId
     * @return the queryId
     */
    [[nodiscard]] QueryId getQueryId() const;

    /**
     * @brief getter for the operatorId
     * @return the operatorId
     */
    [[nodiscard]] OperatorId getOperatorId() const;

    /**
     * @brief getter for the partitionId
     * @return the partitionId
     */
    [[nodiscard]] PartitionId getPartitionId() const;

    /**
     * @brief getter for the getSubpartitionId
     * @return the subpartitionId
     */
    [[nodiscard]] SubpartitionId getSubpartitionId() const;

    [[nodiscard]] std::string toString() const;

    friend std::ostream& operator<<(std::ostream& os, const NesPartition& partition);

    /**
     * @brief The equals operator for the NesPartition. It is not comparing threadIds
     * @param lhs
     * @param rhs
     * @return
     */
    friend bool operator==(const NesPartition& lhs, const NesPartition& rhs);

    friend bool operator<(const NesPartition& lhs, const NesPartition& rhs);

  private:
    QueryId queryId;
    OperatorId operatorId;
    PartitionId partitionId;
    SubpartitionId subpartitionId;
};
}// namespace NES::Network
namespace std {
template<>
struct hash<NES::Network::NesPartition> {
    std::uint64_t operator()(const NES::Network::NesPartition& k) const;
};

}// namespace std

namespace fmt {
template<>
struct formatter<NES::Network::NesPartition> : formatter<std::string> {
    auto format(const NES::Network::NesPartition& partition, format_context& ctx) -> decltype(ctx.out());
};
}//namespace fmt
#endif  // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_NETWORK_NESPARTITION_HPP_
