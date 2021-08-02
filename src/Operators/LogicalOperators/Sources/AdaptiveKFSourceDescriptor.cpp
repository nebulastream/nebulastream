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

#include <Operators/LogicalOperators/Sources/AdaptiveKFSourceDescriptor.hpp>
#include <Sources/AdaptiveKFSource.hpp>

namespace NES {

AdaptiveKFSourceDescriptor::AdaptiveKFSourceDescriptor(SchemaPtr schema, uint64_t numberOfTuplesToProducePerBuffer,
                                                       uint64_t numBuffersToProcess, uint64_t frequency)
    : SourceDescriptor(std::move(schema)), numBuffersToProcess(numBuffersToProcess),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), frequency(frequency){};

AdaptiveKFSourceDescriptor::AdaptiveKFSourceDescriptor(SchemaPtr schema, std::string streamName,
                                                       uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                                       uint64_t frequency)
    : SourceDescriptor(std::move(schema), std::move(streamName)),
      numBuffersToProcess(numBuffersToProcess), numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer),
      frequency(frequency){};

SourceDescriptorPtr AdaptiveKFSourceDescriptor::create(SchemaPtr schema, uint64_t numberOfTuplesToProducePerBuffer,
                                                       uint64_t numBuffersToProcess, uint64_t frequency) {
    return std::make_shared<AdaptiveKFSourceDescriptor>(AdaptiveKFSourceDescriptor(
        std::move(schema), numberOfTuplesToProducePerBuffer, numBuffersToProcess, frequency));
}

SourceDescriptorPtr AdaptiveKFSourceDescriptor::create(SchemaPtr schema, std::string streamName,
                                                       uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                                       uint64_t frequency) {
    return std::make_shared<AdaptiveKFSourceDescriptor>(
        AdaptiveKFSourceDescriptor(std::move(schema), std::move(streamName), numberOfTuplesToProducePerBuffer,
                                   numBuffersToProcess, frequency));
}

bool AdaptiveKFSourceDescriptor::equal(SourceDescriptorPtr const& other) {
    if (!other->instanceOf<AdaptiveKFSourceDescriptor>()) {
        return false;
    }
    auto otherSource = other->as<AdaptiveKFSourceDescriptor>();
    return getSchema()->equals(otherSource->getSchema()) && numBuffersToProcess == otherSource->getNumBuffersToProcess()
        && numberOfTuplesToProducePerBuffer == otherSource->getNumberOfTuplesToProducePerBuffer()
        && frequency == otherSource->getFrequency();
}

uint64_t AdaptiveKFSourceDescriptor::getNumBuffersToProcess() const { return numBuffersToProcess; }
uint64_t AdaptiveKFSourceDescriptor::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }
uint64_t AdaptiveKFSourceDescriptor::getFrequency() const { return frequency; }
std::string AdaptiveKFSourceDescriptor::toString() { return "AdaptiveKFSourceDescriptor()"; }

}// namespace NES