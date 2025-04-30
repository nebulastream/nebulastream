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
#include <../../eigen/Dense>
#include <Execution/Operators/SliceStore/WindowTriggerPredictor/AbstractWindowTriggerPredictor.hpp>

namespace NES::Runtime::Execution
{

class KalmanWindowTriggerPredictor final : public AbstractWindowTriggerPredictor
{
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
};

}
