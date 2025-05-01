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
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>

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

    void processSlice(const OriginId streamId, const Timestamp timestamp, const SequenceNumber sequenceNumber)
    {
        if (!kalmanFilters.contains(streamId))
        {
            initializeKalmanFilter(streamId);
        }

        Eigen::VectorXd measurement(2);
        measurement << timestamp.getRawValue(), sequenceNumber.getRawValue();

        kalmanFilters[streamId].predict(stateTransitionMatrix);
        kalmanFilters[streamId].update(measurement, measurementMatrix);

        // Update the sequence number and timestamp mapping
        sequenceTimestamps[streamId][sequenceNumber] = timestamp;

        std::cout << "Stream ID: " << streamId << ", Processed slice " << sequenceNumber << " at timestamp " << timestamp << std::endl;
    }

    Timestamp getEstimatedTimestamp(const OriginId streamId, const SequenceNumber sequenceNumber)
    {
        if (!kalmanFilters.contains(streamId))
        {
            return Timestamp(std::numeric_limits<int>::max()); // Return a large value if the stream ID is not found
        }

        // If the sequence number is already received, return its timestamp
        if (sequenceTimestamps[streamId].contains(sequenceNumber))
        {
            return sequenceTimestamps[streamId][sequenceNumber];
        }

        // Predict the timestamp for the missing sequence number using the Kalman filter
        const auto lastSequenceNumber = getLastSequenceNumber(streamId);
        const auto lastTimestamp = sequenceTimestamps[streamId][lastSequenceNumber];

        // Predict the timestamp for the missing sequence number
        const auto deltaSequence = sequenceNumber.getRawValue() - lastSequenceNumber.getRawValue();
        const auto deltaTime = deltaSequence * averageTimeInterval(streamId);
        const auto estimatedTimestamp = lastTimestamp + deltaTime;

        return estimatedTimestamp;
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
        sequenceTimestamps[streamId] = std::map<SequenceNumber, Timestamp>();
    }

    SequenceNumber getLastSequenceNumber(const OriginId streamId) const
    {
        if (!sequenceTimestamps.contains(streamId) || sequenceTimestamps.at(streamId).empty())
        {
            return SequenceNumber(-1); // TODO Return -1 if no sequence numbers are available
        }
        return std::prev(sequenceTimestamps.at(streamId).end())->first;
    }

    uint64_t averageTimeInterval(const OriginId streamId) const
    {
        if (!sequenceTimestamps.contains(streamId) || sequenceTimestamps.at(streamId).size() < 2)
        {
            return 1; // Return a default value if not enough data is available
        }
        auto totalTime = Timestamp(0);
        int count = 0;
        auto& timestamps = sequenceTimestamps.at(streamId);
        auto it = timestamps.begin();
        auto prevTimestamp = it->second;
        ++it;
        for (; it != timestamps.end(); ++it)
        {
            totalTime += (it->second - prevTimestamp.getRawValue()).getRawValue();
            prevTimestamp = it->second;
            ++count;
        }
        return totalTime.getRawValue() / count;
    }

    std::map<OriginId, KalmanFilter> kalmanFilters;
    std::map<OriginId, std::map<SequenceNumber, Timestamp>> sequenceTimestamps; // Map stream IDs to sequence number and timestamp mappings
    Eigen::MatrixXd stateTransitionMatrix = Eigen::MatrixXd::Identity(2, 2);
    Eigen::MatrixXd measurementMatrix = Eigen::MatrixXd::Identity(2, 2);
};

}
