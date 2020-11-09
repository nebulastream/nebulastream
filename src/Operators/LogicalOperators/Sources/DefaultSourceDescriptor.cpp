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
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <utility>
namespace NES {

DefaultSourceDescriptor::DefaultSourceDescriptor(SchemaPtr schema,
                                                 uint64_t numbersOfBufferToProduce,
                                                 uint32_t frequency, size_t sourceId)
    : SourceDescriptor(std::move(schema), sourceId), numbersOfBufferToProduce(numbersOfBufferToProduce), frequency(frequency) {}

DefaultSourceDescriptor::DefaultSourceDescriptor(SchemaPtr schema,
                                                 std::string streamName,
                                                 uint64_t numbersOfBufferToProduce,
                                                 uint32_t frequency, size_t sourceId)
    : SourceDescriptor(std::move(schema), std::move(streamName), sourceId), numbersOfBufferToProduce(numbersOfBufferToProduce), frequency(frequency) {}

uint64_t DefaultSourceDescriptor::getNumbersOfBufferToProduce() const {
    return numbersOfBufferToProduce;
}

uint32_t DefaultSourceDescriptor::getFrequency() const {
    return frequency;
}

SourceDescriptorPtr DefaultSourceDescriptor::create(SchemaPtr schema,
                                                    uint64_t numbersOfBufferToProduce,
                                                    uint32_t frequency, size_t sourceId) {
    return std::make_shared<DefaultSourceDescriptor>(DefaultSourceDescriptor(std::move(schema),
                                                                             numbersOfBufferToProduce,
                                                                             frequency, sourceId));
}

SourceDescriptorPtr DefaultSourceDescriptor::create(SchemaPtr schema,
                                                    std::string streamName,
                                                    uint64_t numbersOfBufferToProduce,
                                                    uint32_t frequency, size_t sourceId) {
    return std::make_shared<DefaultSourceDescriptor>(DefaultSourceDescriptor(std::move(schema),
                                                                             std::move(streamName),
                                                                             numbersOfBufferToProduce,
                                                                             frequency, sourceId));
}
bool DefaultSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<DefaultSourceDescriptor>())
        return false;
    auto otherSource = other->as<DefaultSourceDescriptor>();
    return numbersOfBufferToProduce == otherSource->getNumbersOfBufferToProduce() && frequency == otherSource->getFrequency() && getSchema()->equals(otherSource->getSchema());
}

std::string DefaultSourceDescriptor::toString() {
    return "DefaultSourceDescriptor(" + std::to_string(numbersOfBufferToProduce) + ", " + std::to_string(frequency) + ")";
}
}// namespace NES
