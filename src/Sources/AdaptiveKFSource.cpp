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
                                   const uint64_t numSourceLocalBuffers, OperatorId operatorId,
                                   std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(schema, bufferManager, queryManager,
                     operatorId, numSourceLocalBuffers, GatheringMode::FREQUENCY_MODE, std::move(successors)),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), freqRange(2),
      frequency(initialFrequency), freqLastReceived(initialFrequency), kfErrorWindow(20) {
    this->numBuffersToProcess = numBuffersToProcess;
    calculateTotalEstimationErrorDivider(20); // calculate once at init
    this->kFilter.setDefaultValues();
}

std::string AdaptiveKFSource::toString() const {
    std::stringstream ss;
    ss << "ADAPTIVE_KF_SOURCE(SCHEMA(" << schema->toString() << "), freq=" << this->gatheringInterval.count()
       << "ms,"
       << " numBuff=" << this->numBuffersToProcess << ")";
    return ss.str();
}

std::optional<Runtime::TupleBuffer> AdaptiveKFSource::receiveData() {
    NES_DEBUG("AdaptiveKFSource::receiveData called on " << operatorId);
    auto buffer = this->bufferManager->getBufferBlocking();
    fillBuffer(buffer);
    NES_DEBUG("CSVSource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());

    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer;
}

void AdaptiveKFSource::fillBuffer(Runtime::TupleBuffer& buffer) {
    // TODO: remove when moving the source to resemble a memory one
    auto size = buffer.getBufferSize();
    ++size;
    buffer.setNumberOfTuples(size);
}

uint64_t AdaptiveKFSource::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; };

uint64_t AdaptiveKFSource::getFrequency() const { return frequency; }

SourceType AdaptiveKFSource::getType() const { return ADAPTIVE_KF_SOURCE; }

void AdaptiveKFSource::sampleSourceAndFillBuffer(Runtime::TupleBuffer& buf) {
    NES_DEBUG("AdaptiveKFSource::sampleSourceAndFillBuffer called on " << operatorId);
    buf.setNumberOfTuples(0);
    return;
}

void AdaptiveKFSource::decideNewGatheringInterval() {
    auto powerOfEuler = (this->calculateTotalEstimationError() + lambda) / lambda;
    freqDesired = frequency + theta * (1 - std::pow(eulerConstant, powerOfEuler));
    if (desiredFreqInRange()) {
        NES_DEBUG("AdaptiveKFSource::decideNewGatheringInterval old: " << frequency << ", new: " << freqDesired);
        frequency = freqDesired;
    }
}

bool AdaptiveKFSource::desiredFreqInRange() {
    return freqDesired >= (freqLastReceived - (freqRange / 2)) &&
           freqDesired <= (freqLastReceived + (freqRange / 2));
}

long AdaptiveKFSource::updateCurrentEstimationError() {
    auto currentEstimationError = this->kFilter.getEstimationError();
    this->kfErrorWindow.emplace(currentEstimationError);
    NES_DEBUG("AdaptiveKFSource::updateCurrentEstimationError: " << currentEstimationError);
    return currentEstimationError;
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

[[maybe_unused]] void AdaptiveKFSource::setTheta(uint64_t newTheta) {
    NES_DEBUG("AdaptiveKFSource::setTheta old: " << this->theta << ", new: " << newTheta);
    this->theta = newTheta;
}

[[maybe_unused]] void AdaptiveKFSource::setLambda(float newLambda) {
    NES_DEBUG("AdaptiveKFSource::setLambda old: " << this->lambda << ", new: " << newLambda);
    this->lambda = newLambda;
}

}// namespace NES
