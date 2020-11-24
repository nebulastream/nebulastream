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

#include <Operators/LogicalOperators/Sources/YSBSourceDescriptor.hpp>
#include <Sources/YSBSource.hpp>
#include <memory>
#include <utility>

namespace NES {

SourceDescriptorPtr YSBSourceDescriptor::create(uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                                uint64_t frequency, bool endlessRepeat, OperatorId operatorId) {
    return std::make_shared<YSBSourceDescriptor>(
        YSBSourceDescriptor(numberOfTuplesToProducePerBuffer, numBuffersToProcess, frequency, endlessRepeat, operatorId));
}

SourceDescriptorPtr YSBSourceDescriptor::create(std::string streamName, uint64_t numberOfTuplesToProducePerBuffer,
                                                uint64_t numBuffersToProcess, uint64_t frequency, bool endlessRepeat,
                                                OperatorId operatorId) {
    return std::make_shared<YSBSourceDescriptor>(YSBSourceDescriptor(std::move(streamName), numberOfTuplesToProducePerBuffer,
                                                                     numBuffersToProcess, frequency, endlessRepeat, operatorId));
}

YSBSourceDescriptor::YSBSourceDescriptor(uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess, uint64_t frequency,
                                         bool endlessRepeat, OperatorId operatorId)
    : SourceDescriptor(YSBSource::YsbSchema(), operatorId), numBuffersToProcess(numBuffersToProcess),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), frequency(frequency), endlessRepeat(endlessRepeat) {}

YSBSourceDescriptor::YSBSourceDescriptor(std::string streamName, uint64_t numberOfTuplesToProducePerBuffer,
                                         uint64_t numBuffersToProcess, uint64_t frequency, bool endlessRepeat, OperatorId operatorId)
    : SourceDescriptor(YSBSource::YsbSchema(), std::move(streamName), operatorId), numBuffersToProcess(numBuffersToProcess),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), frequency(frequency), endlessRepeat(endlessRepeat) {}

uint64_t YSBSourceDescriptor::getNumBuffersToProcess() const { return numBuffersToProcess; }

uint64_t YSBSourceDescriptor::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

uint64_t YSBSourceDescriptor::getFrequency() const { return frequency; }

bool YSBSourceDescriptor::isEndlessRepeat() const { return endlessRepeat; }

std::string YSBSourceDescriptor::toString() {
    return "YSBSourceDescriptor(" + std::to_string(numberOfTuplesToProducePerBuffer) + "," + ", "
        + std::to_string(numBuffersToProcess) + ", " + std::to_string(frequency) + std::to_string(endlessRepeat) + ")";
}

bool YSBSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<YSBSourceDescriptor>())
        return false;
    auto otherSource = other->as<YSBSourceDescriptor>();
    return numberOfTuplesToProducePerBuffer == otherSource->getNumberOfTuplesToProducePerBuffer()
        && numBuffersToProcess == otherSource->getNumBuffersToProcess() && frequency == otherSource->getFrequency()
        && getSchema()->equals(otherSource->getSchema()) && endlessRepeat == otherSource->isEndlessRepeat();
}

}// namespace NES