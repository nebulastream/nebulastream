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

#ifndef NES_INCLUDE_UTIL_KALMAN_FILTER_HPP_
#define NES_INCLUDE_UTIL_KALMAN_FILTER_HPP_

#include <Eigen/Dense>
#include <Util/CircularBuffer.hpp>
#include <chrono>

namespace NES {

/**
 * @brief A Kalman Filter with functionality to update
 * a frequency, based on the error level during (or after)
 * an update.
 *
 * The KF does a predict-and-update step, where internal
 * state is updated. The last W error levels are kept
 * in-memory, so that they also contribute to the
 * decision-making process.
 *
 * This implementation keeps the terminology and
 * the variable names consistent with the most readily
 * available knowledge resource for KFs, which is wikipedia.
 * The reason is that the original paper is old, so there's
 * lots of names for the different steps and variables.
 */
class KalmanFilter {

  public:
    explicit KalmanFilter(double timeStep,
                          Eigen::MatrixXd F,
                          Eigen::MatrixXd H,
                          Eigen::MatrixXd Q,
                          Eigen::MatrixXd R,
                          Eigen::MatrixXd P,
                          const uint64_t errorWindowSize = 10);
    explicit KalmanFilter(const uint64_t errorWindowSize = 10);

    // initialization methods, empty, only 1 state, only 1 state + timestamp
    void init();// all zeroes
    void init(const Eigen::VectorXd& initialState);
    void init(const Eigen::VectorXd& initialState, double initialTimestamp);
    void setDefaultValues();// create artificial initial values

    // update methods, 1 value vector, 1 value vector + diff. timestep
    void update(const Eigen::VectorXd& measuredValues); // same timestep
    void update(const Eigen::VectorXd& measuredValues, double newTimeStep);// update with timestep
    void update(const Eigen::VectorXd& measuredValues,
                double newTimeStep,
                const Eigen::MatrixXd& A);// update using new timestep and dynamics
    void updateFromTupleBuffer(Runtime::TupleBuffer& buffer);// directly feed a buffer to the filter

    // simple setters/getters for individual fields
    double getCurrentStep();
    Eigen::VectorXd getState();
    Eigen::MatrixXd getError();
    Eigen::MatrixXd getInnovationError();
    double getEstimationError();
    uint64_t getTheta();
    float getLambda();
    void setLambda(float newLambda);

    // frequency related setters
    // needed to set freq/ranger after init
    void setFrequency(std::chrono::milliseconds frequencyInMillis);
    void setFrequencyRange(std::chrono::milliseconds frequencyRange);
    void setFrequencyWithRange(std::chrono::milliseconds frequencyInMillis,
                               std::chrono::milliseconds frequencyRange);
    std::chrono::milliseconds getCurrentFrequency();

    /**
     * @brief calculate new gathering interval using euler number
     * as the smoothing part. The new proposed gathering interval
     * has to stay inside the original frequency range.
     * @return a new gathering interval that we can sleep on
     */
    std::chrono::milliseconds getNewFrequency(); // eq. 7 and 10

    /**
     * @return the total estimation error, calculated
     * from the window. This just exposes it in a
     * public API.
     */
    double getTotalEstimationError();

  protected:
    int m, n;// system model dimensions

    /**
    * Process-specific matrices for a general KF.
    * These are using the names from the original paper.
	*   stateTransitionModel - F
	*   observationModel - H
	*   processNoiseCovariance - Q
	*   measurementNoiseCovariance - R
	*   estimateCovariance - P
    *   kalmanGain - K
    *   iniitalEstimateCovariance - P0
	*/
    Eigen::MatrixXd stateTransitionModel, observationModel, processNoiseCovariance, measurementNoiseCovariance,
        estimateCovariance, kalmanGain, initialEstimateCovariance;
    Eigen::MatrixXd identityMatrix;// identity matrix identityMatrix, on size n

    // estimated state, estimated state +1
    Eigen::VectorXd xHat, xHatNew;

    // error between predict/update
    Eigen::VectorXd innovationError;// eq. 3

    // time-related members
    double timeStep;
    double initialTimestamp;
    double currentTime;
    double estimationError;// eq. 8

    /**
     * @brief used to give lower/upper bounds on freq.
     * Paper is not clear on the magnitude (size) of
     * the range, this can be determined in tests later.
     */
    std::chrono::milliseconds freqRange{8000}; // allowed to change by +4s/-4s
    std::chrono::milliseconds frequency{1000}; // currently in use
    std::chrono::milliseconds freqLastReceived{1000}; // from coordinator

    /**
     * @brief control units for changing the new
     * frequency. Theta (θ) is static according
     * to the paper in Jain et al.
     */
    const uint64_t theta = 2; // θ = 2 in all experiments
    float lambda = 0.6; // λ = 0.6 in most experiments

    /**
     * @brief _e_ constant, used to calculate
     * magnitude of change for the new
     * frequency estimation.
     */
    const double eulerConstant = std::exp(1.0);

    /**
     * @brief buffer of residual error from KF
     */
    CircularBuffer<float> kfErrorWindow;

    /**
     * Calculates the current estimation error.
     * Uses the last W errors stored in the
     * history window in kfErrorWindow. Basically
     * sum all errors in the window and divide
     * them by the totalEstimationErrorDivider.
     * @return the total error over the history window
     */
    float calculateTotalEstimationError(); // eq. 9

    /**
     * Calculate the divider of the total estimation
     * error. It stays the same across history,
     * so it can be calculated once, during
     * initialization.
     * @return the current estimation error divider
     */
    float totalEstimationErrorDivider;
    void calculateTotalEstimationErrorDivider(int size);// eq. 9 (divider, calc. once)

};// class KalmanFilter

}// namespace NES

#endif // NES_INCLUDE_UTIL_KALMAN_FILTER_HPP_
