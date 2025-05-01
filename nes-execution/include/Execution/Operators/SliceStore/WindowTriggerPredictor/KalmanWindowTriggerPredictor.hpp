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

#pragma once

// TODO add to cmake as library
#include <iostream>
#include <map>
#include <../../eigen/Dense>
#include <Execution/Operators/SliceStore/WindowTriggerPredictor/AbstractWindowTriggerPredictor.hpp>
#include "Identifiers/Identifiers.hpp"

namespace NES::Runtime::Execution
{

class KalmanWindowTriggerPredictor final : public AbstractWindowTriggerPredictor
{
public:
    struct KalmanFilter
    {
        KalmanFilter(
            const Eigen::VectorXd& initialState,
            const Eigen::MatrixXd& initialCovariance,
            const Eigen::MatrixXd& processNoise,
            const Eigen::MatrixXd& measurementNoise)
            : state(initialState), covariance(initialCovariance), processNoise(processNoise), measurementNoise(measurementNoise)
        {
        }

        void predict(const Eigen::MatrixXd& stateTransitionMatrix, const Eigen::VectorXd& controlInput = Eigen::VectorXd::Zero(1))
        {
            state = stateTransitionMatrix * state + controlInput;
            covariance = stateTransitionMatrix * covariance * stateTransitionMatrix.transpose() + processNoise;
        }

        void update(const Eigen::VectorXd& measurement, const Eigen::MatrixXd& measurementMatrix)
        {
            const Eigen::MatrixXd kalmanGain = covariance * measurementMatrix.transpose()
                * (measurementMatrix * covariance * measurementMatrix.transpose() + measurementNoise).inverse();
            state = state + kalmanGain * (measurement - measurementMatrix * state);
            covariance = (Eigen::MatrixXd::Identity(covariance.rows(), covariance.cols()) - kalmanGain * measurementMatrix) * covariance;
        }

        [[nodiscard]] Eigen::VectorXd getState() const { return state; }

        [[nodiscard]] int getEstimatedTimestamp() const { return static_cast<int>(state(0)); }

    private:
        Eigen::VectorXd state;
        Eigen::MatrixXd covariance;
        Eigen::MatrixXd processNoise;
        Eigen::MatrixXd measurementNoise;
    };

    void processSlice(const OriginId streamId, const int timestamp, const SequenceNumber sequenceNumber)
    {
        if (!kalmanFilters.contains(streamId))
        {
            initializeKalmanFilter(streamId);
        }

        Eigen::VectorXd measurement(2);
        measurement << timestamp, sequenceNumber.getRawValue();

        kalmanFilters[streamId].predict(stateTransitionMatrix);
        kalmanFilters[streamId].update(measurement, measurementMatrix);

        const int estimatedTimestamp = kalmanFilters[streamId].getEstimatedTimestamp();
        std::cout << "Stream ID: " << streamId << ", Estimated timestamp for slice " << sequenceNumber.getRawValue() << ": "
                  << estimatedTimestamp << std::endl;
    }

    int getEstimatedTimestamp(const OriginId streamId, int sequenceNumber)
    {
        if (!kalmanFilters.contains(streamId))
        {
            return std::numeric_limits<int>::max(); // Return a large value if the stream ID is not found
        }
        return kalmanFilters[streamId].getEstimatedTimestamp();
    }

private:
    void initializeKalmanFilter(const OriginId streamId)
    {
        Eigen::VectorXd initialState(2);
        initialState << 0, 0; // Initial state: [timestamp, sequenceNumber]

        Eigen::MatrixXd initialCovariance(2, 2);
        initialCovariance << 1, 0, 0, 1;

        Eigen::MatrixXd processNoise(2, 2);
        processNoise << 0.1, 0, 0, 0.1;

        Eigen::MatrixXd measurementNoise(2, 2);
        measurementNoise << 0.1, 0, 0, 0.1;

        kalmanFilters[streamId] = KalmanFilter(initialState, initialCovariance, processNoise, measurementNoise);
    }

    std::map<OriginId, KalmanFilter> kalmanFilters;
    Eigen::MatrixXd stateTransitionMatrix = Eigen::MatrixXd::Identity(2, 2);
    Eigen::MatrixXd measurementMatrix = Eigen::MatrixXd::Identity(2, 2);
};

}
