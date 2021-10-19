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

#ifndef INCLUDE_ADAPTIVEKFSOURCE_HPP_
#define INCLUDE_ADAPTIVEKFSOURCE_HPP_

#include <Sources/AdaptiveSource.hpp>
#include <Util/CircularBuffer.hpp>
#include <Util/KalmanFilter.hpp>
#include <cmath>

namespace NES {

class AdaptiveKFSource : public AdaptiveSource {
  public:
    explicit AdaptiveKFSource(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager,
                              Runtime::QueryManagerPtr queryManager, const uint64_t numBuffersToProcess,
                              uint64_t numberOfTuplesToProducePerBuffer, uint64_t frequency,
                              const uint64_t numSourceLocalBuffers, OperatorId operatorId);

    std::string toString() const override;

    /**
     * @brief Get number of tuples per buffer
     */
    uint64_t getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief Get current sampling frequency
     */
    uint64_t getFrequency() const;

    /**
     * @brief Get source type
     * @return source type
     */
    SourceType getType() const override;

  private:
    void sampleSourceAndFillBuffer(Runtime::TupleBuffer&) override;
    void decideNewGatheringInterval() override;// eq. 10 in the paper

    // paper equations as methods
    bool desiredFreqInRange(); // eq. 7
    long updateCurrentEstimationError(); // eq. 8 (after insertion to kf)
    long calculateTotalEstimationError(); // eq. 9
    void calculateTotalEstimationErrorDivider(int size);// eq. 9 (divider, calculated at init)

    uint64_t numberOfTuplesToProducePerBuffer;

    /**
     * @brief used to give lower/upper bounds on freq.
     * Paper is not clear on the magnitude (size) of
     * the range, this can be determined in tests later.
     */
    uint64_t freqRange = 2;   // freq +- 2
    uint64_t frequency;       // currently in use
    uint64_t freqLastReceived;// from coordinator
    uint64_t freqDesired;     // from KF prediction

    /**
     * @brief control units for changing the new
     * frequency. Theta (θ) is static according
     * to the paper. Lambda (λ) is parameterizable.
     */
    uint64_t theta = 2; // θ = 2 in all experiments
    float lambda = 0.6; // λ = 0.6 in most experiments

    /**
     * @brief control lambda and theta
     * after initialization. These help
     * test the source the same way as
     * the paper. _Not_ intended to
     * control change _during_ an experiment.
     */
    [[maybe_unused]] void setTheta(uint64_t newTheta);
    [[maybe_unused]] void setLambda(float newLambda);

    /**
     * @brief _e_ constant, used to calculate
     * magnitude of change for the new
     * frequency estimation.
     */
    const double eulerConstant = std::exp(1.0);

    /**
     * @brief buffer of residual error from KF
     */
    CircularBuffer<long> kfErrorWindow;

    /**
     * @brief used in eq. 9 to weight the total
     * estimation error. Since it's fixed to the
     * size of the kfErrorWindow, it can be
     * calculated once.
     */
    float totalEstimationErrorDivider;

    /**
     * @brief the KF associated with a source.
     * We use default values for initialization.
     * TODO: allow tuning to the measurement domain
     */
    KalmanFilter kFilter;
};

using AdaptiveKFSourcePtr = std::shared_ptr<AdaptiveKFSource>;

}// namespace NES
#endif /* INCLUDE_ADAPTIVEKFSOURCE_HPP_ */
