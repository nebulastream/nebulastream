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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <fmt/format.h>
#include <utility>

namespace NES::Network {

NetworkSourceDescriptor::NetworkSourceDescriptor(SchemaPtr schema,
                                                 const NesPartition& nesPartition,
                                                 const NodeLocation& nodeLocation,
                                                 std::chrono::milliseconds waitTime,
                                                 uint32_t retryTimes,
                                                 DecomposedQueryPlanVersion version,
                                                 OperatorId uniqueNetworkSourceId)
    : SourceDescriptor(std::move(schema)), nesPartition(nesPartition), nodeLocation(nodeLocation), waitTime(waitTime),
      retryTimes(retryTimes), version(version), uniqueNetworkSourceId(uniqueNetworkSourceId) {}

SourceDescriptorPtr NetworkSourceDescriptor::create(SchemaPtr schema,
                                                    const NesPartition& nesPartition,
                                                    const NodeLocation& nodeLocation,
                                                    std::chrono::milliseconds waitTime,
                                                    uint32_t retryTimes,
                                                    DecomposedQueryPlanVersion version,
                                                    OperatorId uniqueNetworkSourceId) {
    return std::make_shared<NetworkSourceDescriptor>(NetworkSourceDescriptor(std::move(schema),
                                                                             nesPartition,
                                                                             nodeLocation,
                                                                             waitTime,
                                                                             retryTimes,
                                                                             version,
                                                                             uniqueNetworkSourceId));
}

bool NetworkSourceDescriptor::equal(SourceDescriptorPtr const& other) const {
    if (!other->instanceOf<NetworkSourceDescriptor>()) {
        return false;
    }
    auto otherNetworkSource = other->as<NetworkSourceDescriptor>();
    return schema->equals(otherNetworkSource->schema) && nesPartition == otherNetworkSource->nesPartition;
}

std::string NetworkSourceDescriptor::toString() const {
    return fmt::format("NetworkSourceDescriptor{{Version={};Partition={};NetworkSinkNodeLocation={}}}",
                       version,
                       nesPartition.toString(),
                       nodeLocation.createZmqURI());
}

NesPartition NetworkSourceDescriptor::getNesPartition() const { return nesPartition; }

NodeLocation NetworkSourceDescriptor::getNodeLocation() const { return nodeLocation; }

std::chrono::milliseconds NetworkSourceDescriptor::getWaitTime() const { return waitTime; }

uint8_t NetworkSourceDescriptor::getRetryTimes() const { return retryTimes; }

uint16_t NetworkSourceDescriptor::getVersion() const { return version; }

SourceDescriptorPtr NetworkSourceDescriptor::copy() {
    auto copy = NetworkSourceDescriptor::create(schema->copy(),
                                                nesPartition,
                                                nodeLocation,
                                                waitTime,
                                                retryTimes,
                                                version,
                                                uniqueNetworkSourceId);
    return copy;
}
OperatorId NetworkSourceDescriptor::getUniqueId() const { return uniqueNetworkSourceId; }

void NetworkSourceDescriptor::markForReuse() {
    NES_ASSERT(!markedForReuse, "NetworkSourceDescriptor::markForReuse: already marked for reuse");
    markedForReuse = true;
}

bool NetworkSourceDescriptor::isMarkedForReuse() {
    if (markedForReuse) {
        markedForReuse = false;
        return true;
    }
    return false;
}
}// namespace NES::Network
