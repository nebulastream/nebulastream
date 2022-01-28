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
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <utility>

namespace NES::Network {

NetworkSourceDescriptor::NetworkSourceDescriptor(SchemaPtr schema,
                                                 NesPartition nesPartition,
                                                 NodeLocation nodeLocation,
                                                 std::chrono::milliseconds waitTime,
                                                 uint32_t retryTimes)
    : SourceDescriptor(std::move(schema)), nesPartition(nesPartition), nodeLocation(nodeLocation), waitTime(waitTime),
      retryTimes(retryTimes) {}

SourceDescriptorPtr NetworkSourceDescriptor::create(SchemaPtr schema,
                                                    NesPartition nesPartition,
                                                    NodeLocation nodeLocation,
                                                    std::chrono::milliseconds waitTime,
                                                    uint32_t retryTimes) {
    return std::make_shared<NetworkSourceDescriptor>(
        NetworkSourceDescriptor(std::move(schema), nesPartition, nodeLocation, waitTime, retryTimes));
}

bool NetworkSourceDescriptor::equal(SourceDescriptorPtr const& other) {
    if (!other->instanceOf<NetworkSourceDescriptor>()) {
        return false;
    }
    auto otherNetworkSource = other->as<NetworkSourceDescriptor>();
    return schema->equals(otherNetworkSource->schema) && nesPartition == otherNetworkSource->nesPartition;
}

std::string NetworkSourceDescriptor::toString() {
    return "NetworkSourceDescriptor{" + nodeLocation.createZmqURI() + " " + nesPartition.toString() + "}";
}

NesPartition NetworkSourceDescriptor::getNesPartition() const { return nesPartition; }

NodeLocation NetworkSourceDescriptor::getNodeLocation() const { return nodeLocation; }

std::chrono::milliseconds NetworkSourceDescriptor::getWaitTime() const { return waitTime; }

uint8_t NetworkSourceDescriptor::getRetryTimes() const { return retryTimes; }

SourceDescriptorPtr NetworkSourceDescriptor::copy() {
    auto copy = NetworkSourceDescriptor::create(schema->copy(), nesPartition, nodeLocation, waitTime, retryTimes);
    return copy;
}

}// namespace NES::Network