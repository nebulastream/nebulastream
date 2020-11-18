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

#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <utility>

namespace NES {
namespace Network {

NetworkSourceDescriptor::NetworkSourceDescriptor(SchemaPtr schema, NesPartition nesPartition)
    : SourceDescriptor(std::move(schema), nesPartition.getOperatorId()), nesPartition(nesPartition) {}

SourceDescriptorPtr NetworkSourceDescriptor::create(SchemaPtr schema, NesPartition nesPartition) {
    return std::make_shared<NetworkSourceDescriptor>(NetworkSourceDescriptor(std::move(schema), nesPartition));
}

bool NetworkSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<NetworkSourceDescriptor>()) {
        return false;
    }
    auto otherNetworkSource = other->as<NetworkSourceDescriptor>();
    return schema == otherNetworkSource->schema && nesPartition == otherNetworkSource->nesPartition;
}

std::string NetworkSourceDescriptor::toString() { return "NetworkSourceDescriptor()"; }

NesPartition NetworkSourceDescriptor::getNesPartition() const { return nesPartition; }

}// namespace Network
}// namespace NES