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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <utility>
namespace NES {

BinarySourceDescriptor::BinarySourceDescriptor(SchemaPtr schema, std::string filePath, SourceId sourceId) : SourceDescriptor(std::move(schema), sourceId), filePath(std::move(filePath)) {}

BinarySourceDescriptor::BinarySourceDescriptor(SchemaPtr schema, std::string streamName, std::string filePath, SourceId sourceId) : SourceDescriptor(std::move(schema), std::move(streamName), sourceId), filePath(std::move(filePath)) {}

SourceDescriptorPtr BinarySourceDescriptor::create(SchemaPtr schema, std::string filePath, SourceId sourceId) {
    return std::make_shared<BinarySourceDescriptor>(BinarySourceDescriptor(std::move(schema), std::move(filePath), sourceId));
}

SourceDescriptorPtr BinarySourceDescriptor::create(SchemaPtr schema, std::string streamName, std::string filePath, SourceId sourceId) {
    return std::make_shared<BinarySourceDescriptor>(BinarySourceDescriptor(std::move(schema), std::move(streamName), std::move(filePath), sourceId));
}

const std::string& BinarySourceDescriptor::getFilePath() const {
    return filePath;
}

bool BinarySourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<BinarySourceDescriptor>())
        return false;
    auto otherDefaultSource = other->as<BinarySourceDescriptor>();
    return filePath == otherDefaultSource->getFilePath() && getSchema()->equals(otherDefaultSource->getSchema());
}

std::string BinarySourceDescriptor::toString() {
    return "BinarySourceDescriptor(" + filePath + ")";
}
}// namespace NES
