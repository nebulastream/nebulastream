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
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <utility>
namespace NES {

LogicalStreamSourceDescriptor::LogicalStreamSourceDescriptor(std::string streamName, SourceId sourceId)
    : SourceDescriptor(Schema::create(), std::move(streamName), sourceId) {}

SourceDescriptorPtr LogicalStreamSourceDescriptor::create(std::string streamName, SourceId sourceId) {
    return std::make_shared<LogicalStreamSourceDescriptor>(LogicalStreamSourceDescriptor(std::move(streamName), sourceId));
}

bool LogicalStreamSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<LogicalStreamSourceDescriptor>()) {
        return false;
    }
    auto otherLogicalStreamSource = other->as<LogicalStreamSourceDescriptor>();
    return getStreamName() == otherLogicalStreamSource->getStreamName();
}

std::string LogicalStreamSourceDescriptor::toString() {
    return "LogicalStreamSourceDescriptor()";
}
}// namespace NES