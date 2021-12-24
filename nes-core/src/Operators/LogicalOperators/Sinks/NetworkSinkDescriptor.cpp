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

#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <utility>

namespace NES::Network {

NetworkSinkDescriptor::NetworkSinkDescriptor(NodeLocation nodeLocation,
                                             NesPartition nesPartition,
                                             std::chrono::milliseconds waitTime,
                                             uint32_t retryTimes,
                                             OperatorId globalOperatorId)
    : nodeLocation(std::move(nodeLocation)), nesPartition(nesPartition), waitTime(waitTime), retryTimes(retryTimes), globalOperatorId(globalOperatorId) {}

SinkDescriptorPtr NetworkSinkDescriptor::create(NodeLocation nodeLocation,
                                                NesPartition nesPartition,
                                                std::chrono::milliseconds waitTime,
                                                uint32_t retryTimes,
                                                OperatorId globalOperatorId ) {
    return std::make_shared<NetworkSinkDescriptor>(
        NetworkSinkDescriptor(std::move(nodeLocation), nesPartition, waitTime, retryTimes, globalOperatorId));
}

bool NetworkSinkDescriptor::equal(SinkDescriptorPtr const& other) {
    if (!other->instanceOf<NetworkSinkDescriptor>()) {
        return false;
    }
    auto otherSinkDescriptor = other->as<NetworkSinkDescriptor>();
    return (nesPartition == otherSinkDescriptor->nesPartition) && (nodeLocation == otherSinkDescriptor->nodeLocation)
        && (waitTime == otherSinkDescriptor->waitTime) && (retryTimes == otherSinkDescriptor->retryTimes);
}

std::string NetworkSinkDescriptor::toString() {
    return "NetworkSinkDescriptor(partition=" + nesPartition.toString() + ";nodeLocation=" + nodeLocation.createZmqURI() + ")";
}

NodeLocation NetworkSinkDescriptor::getNodeLocation() const { return nodeLocation; }

NesPartition NetworkSinkDescriptor::getNesPartition() const { return nesPartition; }

std::chrono::milliseconds NetworkSinkDescriptor::getWaitTime() const { return waitTime; }

uint8_t NetworkSinkDescriptor::getRetryTimes() const { return retryTimes; }

    OperatorId NetworkSinkDescriptor::getGlobalOperatorId() { return globalOperatorId; }

}// namespace NES::Network