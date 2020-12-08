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

#include <Sources/AdaptiveKFSource.hpp>

namespace NES {

AdaptiveKFSource::AdaptiveKFSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                   const uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                   uint64_t initialFrequency, bool endlessRepeat, OperatorId operatorId)
    : AdaptiveSource(schema, bufferManager, queryManager, initialFrequency, operatorId), numBuffersToProcess(numBuffersToProcess),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), endlessRepeat(endlessRepeat),
      frequency(initialFrequency), freqLastReceived(initialFrequency), freqRange(2), kfErrorWindow(20) {
    calculateTotalEstimationErrorDivider(20);
}

const std::string AdaptiveKFSource::toString() const { return std::string(); }

void AdaptiveKFSource::sampleSourceAndFillBuffer(TupleBuffer& buffer) {
    buffer;
    return;
}

void AdaptiveKFSource::decideNewGatheringInterval() {
    if (desiredFreqInRange()) {
        frequency = freqDesired;
    }
}

bool AdaptiveKFSource::desiredFreqInRange() {
    return freqDesired >= (freqLastReceived - (freqRange / 2)) && freqDesired <= (freqLastReceived + (freqRange / 2));
}

long AdaptiveKFSource::calculateTotalEstimationError() {
    long j = 1; // eq. 9 iterator
    long totalError = 0;
    for (auto errorValue : kfErrorWindow) {
        totalError += (errorValue / j);
        ++j;
    }
    return totalError / totalEstimationErrorDivider;
}

void AdaptiveKFSource::calculateTotalEstimationErrorDivider(int size) {
    totalEstimationErrorDivider = 0;
    for (int i = 1; i <= size; ++i) {
        totalEstimationErrorDivider += (1 / i);
    }
}

}// namespace NES
