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
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <chrono>
#include <utility>
namespace NES {

DefaultSourceDescriptor::DefaultSourceDescriptor(SchemaPtr schema, uint64_t numbersOfBufferToProduce, uint64_t frequency)
    : SourceDescriptor(std::move(schema)), numbersOfBufferToProduce(numbersOfBufferToProduce), frequency(frequency) {}

DefaultSourceDescriptor::DefaultSourceDescriptor(SchemaPtr schema,
                                                 std::string streamName,
                                                 uint64_t numbersOfBufferToProduce,
                                                 uint64_t frequency)
    : SourceDescriptor(std::move(schema), std::move(streamName)), numbersOfBufferToProduce(numbersOfBufferToProduce),
      frequency(frequency) {}

uint64_t DefaultSourceDescriptor::getNumbersOfBufferToProduce() const { return numbersOfBufferToProduce; }

std::chrono::milliseconds DefaultSourceDescriptor::getFrequency() const { return frequency; }
uint64_t DefaultSourceDescriptor::getFrequencyCount() const { return frequency.count(); }

SourceDescriptorPtr DefaultSourceDescriptor::create(SchemaPtr schema, uint64_t numbersOfBufferToProduce, uint64_t frequency) {
    return std::make_shared<DefaultSourceDescriptor>(
        DefaultSourceDescriptor(std::move(schema), numbersOfBufferToProduce, frequency));
}

SourceDescriptorPtr
DefaultSourceDescriptor::create(SchemaPtr schema, std::string streamName, uint64_t numbersOfBufferToProduce, uint64_t frequency) {
    return std::make_shared<DefaultSourceDescriptor>(
        DefaultSourceDescriptor(std::move(schema), std::move(streamName), numbersOfBufferToProduce, frequency));
}
bool DefaultSourceDescriptor::equal(SourceDescriptorPtr const& other) {
    if (!other->instanceOf<DefaultSourceDescriptor>()) {
        return false;
    }
    auto otherSource = other->as<DefaultSourceDescriptor>();
    return numbersOfBufferToProduce == otherSource->getNumbersOfBufferToProduce() && frequency == otherSource->getFrequency()
        && getSchema()->equals(otherSource->getSchema());
}

std::string DefaultSourceDescriptor::toString() {
    return "DefaultSourceDescriptor(" + std::to_string(numbersOfBufferToProduce) + ", " + std::to_string(frequency.count())
        + "ms)";
}
SourceDescriptorPtr DefaultSourceDescriptor::copy() {
    auto copy = DefaultSourceDescriptor::create(schema->copy(),
                                                std::move(logicalSourceName),
                                                numbersOfBufferToProduce,
                                                frequency.count());
    copy->setPhysicalSourceName(physicalSourceName);
    return copy;
}
}// namespace NES
