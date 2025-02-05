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

#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <fmt/format.h>
#include <utility>

namespace NES::Network {

NetworkSinkDescriptor::NetworkSinkDescriptor(const NodeLocation& nodeLocation,
                                             const NesPartition& nesPartition,
                                             std::chrono::milliseconds waitTime,
                                             uint32_t retryTimes,
                                             DecomposedQueryPlanVersion version,
                                             uint64_t numberOfOrigins,
                                             OperatorId uniqueId,
                                             OperatorId downstreamLogicalOperatorId)
    : SinkDescriptor(numberOfOrigins), nodeLocation(nodeLocation), nesPartition(nesPartition), waitTime(waitTime),
      retryTimes(retryTimes), version(version), uniqueNetworkSinkId(uniqueId), downstreamLogicalOperatorId(downstreamLogicalOperatorId) {}

SinkDescriptorPtr NetworkSinkDescriptor::create(const NodeLocation& nodeLocation,
                                                const NesPartition& nesPartition,
                                                std::chrono::milliseconds waitTime,
                                                uint32_t retryTimes,
                                                DecomposedQueryPlanVersion version,
                                                uint64_t numberOfOrigins,
                                                OperatorId uniqueId,
                                                OperatorId downstreamLogicalOperatorId) {
    return std::make_shared<NetworkSinkDescriptor>(
        NetworkSinkDescriptor(nodeLocation, nesPartition, waitTime, retryTimes, version, numberOfOrigins, uniqueId, downstreamLogicalOperatorId));
}

bool NetworkSinkDescriptor::equal(SinkDescriptorPtr const& other) {
    if (!other->instanceOf<NetworkSinkDescriptor>()) {
        return false;
    }
    auto otherSinkDescriptor = other->as<NetworkSinkDescriptor>();
    return (nesPartition == otherSinkDescriptor->nesPartition) && (nodeLocation == otherSinkDescriptor->nodeLocation)
        && (waitTime == otherSinkDescriptor->waitTime) && (retryTimes == otherSinkDescriptor->retryTimes)
        && (version == otherSinkDescriptor->version) && (uniqueNetworkSinkId == otherSinkDescriptor->uniqueNetworkSinkId);
}

std::string NetworkSinkDescriptor::toString() const {
    return fmt::format("NetworkSinkDescriptor{{Version={};Partition={};NetworkSourceNodeLocation={}}}",
                       version,
                       nesPartition.toString(),
                       nodeLocation.createZmqURI());
}

NodeLocation NetworkSinkDescriptor::getNodeLocation() const { return nodeLocation; }

NesPartition NetworkSinkDescriptor::getNesPartition() const { return nesPartition; }

std::chrono::milliseconds NetworkSinkDescriptor::getWaitTime() const { return waitTime; }

uint8_t NetworkSinkDescriptor::getRetryTimes() const { return retryTimes; }

uint16_t NetworkSinkDescriptor::getVersion() const { return version; }

OperatorId NetworkSinkDescriptor::getUniqueId() const { return uniqueNetworkSinkId; }

OperatorId NetworkSinkDescriptor::getDownstreamOperatorId() const { return downstreamLogicalOperatorId; }

}// namespace NES::Network
