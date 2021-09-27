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
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/AdaptiveKFSource.hpp>

namespace NES {

// TODO: change/parameterize the size of the kf-window to other than 20
AdaptiveKFSource::AdaptiveKFSource(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager,
                                   Runtime::QueryManagerPtr queryManager, const uint64_t numberOfTuplesToProducePerBuffer,
                                   uint64_t numBuffersToProcess, uint64_t initialFrequency,
                                   const uint64_t numSourceLocalBuffers, OperatorId operatorId)
    : AdaptiveSource(schema, bufferManager, queryManager, initialFrequency,
                     operatorId, numSourceLocalBuffers, GatheringMode::FREQUENCY_MODE),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), freqRange(2),
      frequency(initialFrequency), freqLastReceived(initialFrequency), kfErrorWindow(20) {
    this->numBuffersToProcess = numBuffersToProcess;
    calculateTotalEstimationErrorDivider(20); // calculate once at init
    this->kFilter.setDefaultValues();
}

std::string AdaptiveKFSource::toString() const { return std::string(); }

uint64_t AdaptiveKFSource::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; };

uint64_t AdaptiveKFSource::getFrequency() const { return frequency; }

SourceType AdaptiveKFSource::getType() const { return ADAPTIVE_KF_SOURCE; }

void AdaptiveKFSource::sampleSourceAndFillBuffer(Runtime::TupleBuffer& buf) {
    buf.setNumberOfTuples(0);
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

long AdaptiveKFSource::calculateCurrentEstimationError() {
    return 0;
}

long AdaptiveKFSource::calculateTotalEstimationError() {
    long j = 1;// eq. 9 iterator
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
