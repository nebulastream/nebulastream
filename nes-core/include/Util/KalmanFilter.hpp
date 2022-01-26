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

#ifndef NES_INCLUDE_UTIL_KALMANFILTER_HPP_
#define NES_INCLUDE_UTIL_KALMANFILTER_HPP_

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
    /**
     * Full c-tor of a filter.
     * The parameters use the mathematical names
     * of the matrices.
     *
     * @param timeStep
     * @param F
     * @param H
     * @param Q
     * @param R
     * @param P
     * @param errorWindowSize
     */
    explicit KalmanFilter(double timeStep,
                          Eigen::MatrixXd F,
                          Eigen::MatrixXd H,
                          Eigen::MatrixXd Q,
                          Eigen::MatrixXd R,
                          Eigen::MatrixXd P,
                          const uint64_t errorWindowSize = 10);

    /**
     * Simple c-tor of a filter.
     * Only uses the history window size.
     * Everything else is initialized to
     * a default set of parameters.
     *
     * @param errorWindowSize
     */
    explicit KalmanFilter(const uint64_t errorWindowSize = 10);

    /**
     * Initialize the matrices in a filter.
     * The init method can also be called
     * with a prepared initialState vector
     * as well as an initialTimestamp.
     */
    void init();// all zeroes
    void init(const Eigen::VectorXd& initialState);
    void init(const Eigen::VectorXd& initialState, double initialTimestamp);

    /**
     * create artificial initial values
     */
    void setDefaultValues();

    /**
     * Update methods, with different signatures.
     * 1st - use only a vector of measured values
     * 2nd - vector of values + timestep for updates
     * 3rd - values + timestem + dynamics matrix
     * @param measuredValues
     */
    void update(const Eigen::VectorXd& measuredValues); // same timestep
    void update(const Eigen::VectorXd& measuredValues, double newTimeStep);// update with timestep
    void update(const Eigen::VectorXd& measuredValues,
                double newTimeStep,
                const Eigen::MatrixXd& A);// update using new timestep and dynamics

    /**
     * Update method, using a full tuple buffer as input.
     * @param buffer
     */
    void updateFromTupleBuffer(Runtime::TupleBuffer& buffer);

    // simple setters/getters for individual fields
    double getCurrentStep();
    Eigen::VectorXd getState();
    Eigen::MatrixXd getError();
    Eigen::MatrixXd getInnovationError();
    double getEstimationError();
    uint64_t getTheta();
    float getLambda();
    void setLambda(float newLambda);

    /**
     * Frequency related setters.
     * @param frequencyInMillis
     */
    void setFrequency(std::chrono::milliseconds frequencyInMillis);
    void setFrequencyRange(std::chrono::milliseconds frequencyRange);
    void setFrequencyWithRange(std::chrono::milliseconds frequencyInMillis,
                               std::chrono::milliseconds frequencyRange);

    /**
     * Get current frequency.
     * @return frequency in millis
     */
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
    void calculateTotalEstimationErrorDivider(int size);// eq. 9 (divider, calc. once)

    /**
     * The divider used whenever an update
     * on the total estimation error happens.
     * It's calculated once on init. Depends
     * on the size of the history window.
     */
    float totalEstimationErrorDivider;

    /**
     * System model dimensions.
     * These are used to initialize
     * the various system matrices.
     */
    int m, n;

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

    /**
     * Estimated state, estimated state in timestep+1
     */
    Eigen::VectorXd xHat, xHatNew;

    /**
     * Error between predict/update
     */
    Eigen::VectorXd innovationError;// eq. 3

    /**
     * Timestep used in updates.
     * This is needed to create special
     * versions of KFs.
     */
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

};// class KalmanFilter

}// namespace NES

#endif  // NES_INCLUDE_UTIL_KALMANFILTER_HPP_
