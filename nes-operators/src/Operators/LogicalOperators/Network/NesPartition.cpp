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

#include <Operators/LogicalOperators/Network/NesPartition.hpp>
#include <fmt/core.h>
#include <functional>
#include <ostream>

namespace NES::Network {

NesPartition::NesPartition(SharedQueryId sharedQueryId,
                           OperatorId operatorId,
                           PartitionId partitionId,
                           SubpartitionId subpartitionId)
    : sharedQueryId(sharedQueryId), operatorId(operatorId), partitionId(partitionId), subpartitionId(subpartitionId) {}

PartitionId NesPartition::getPartitionId() const { return partitionId; }

OperatorId NesPartition::getOperatorId() const { return operatorId; }

SharedQueryId NesPartition::getQueryId() const { return sharedQueryId; }

SubpartitionId NesPartition::getSubpartitionId() const { return subpartitionId; }

std::string NesPartition::toString() const {
    return fmt::format("{}::{}::{}::{}", sharedQueryId, operatorId, partitionId, subpartitionId);
}
bool operator<(const NesPartition& lhs, const NesPartition& rhs) {
    return lhs.sharedQueryId < rhs.sharedQueryId && lhs.operatorId < rhs.operatorId && lhs.partitionId < rhs.partitionId
        && lhs.subpartitionId < rhs.subpartitionId;
}

bool operator==(const NesPartition& lhs, const NesPartition& rhs) {
    return lhs.sharedQueryId == rhs.sharedQueryId && lhs.operatorId == rhs.operatorId && lhs.partitionId == rhs.partitionId
        && lhs.subpartitionId == rhs.subpartitionId;
}
std::ostream& operator<<(std::ostream& os, const NesPartition& partition) {
    os << partition.toString();
    return os;
}
}// namespace NES::Network

std::uint64_t std::hash<NES::Network::NesPartition>::operator()(const NES::Network::NesPartition& k) const {
    using std::hash;

    // Hash function for the NesPartition
    // Compute individual hash values of the Ints and combine them using XOR and bit shifting:
    return ((hash<NES::SharedQueryId>()(k.getQueryId()) ^ (hash<NES::OperatorId>()(k.getOperatorId()) << 1)) >> 1)
        ^ ((hash<NES::PartitionId>()(k.getPartitionId()) ^ (hash<NES::SubpartitionId>()(k.getSubpartitionId()) << 1)) >> 1);
}

auto fmt::formatter<NES::Network::NesPartition>::format(const NES::Network::NesPartition& partition, fmt::format_context& ctx)
    -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(),
                          "query Id:{} OperatorId:{} PartitionId: {} SubpartitionID: {}",
                          partition.getQueryId(),
                          partition.getOperatorId(),
                          partition.getPartitionId(),
                          partition.getSubpartitionId());
}
