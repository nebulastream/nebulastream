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

namespace NES {

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

    void init();// all zeroes
    void init(const Eigen::VectorXd& initialState);
    void init(const Eigen::VectorXd& initialState, double initialTimestamp);
    void setDefaultValues();// create artificial initial values

    void update(const Eigen::VectorXd& measuredValues);                    // same timestep
    void update(const Eigen::VectorXd& measuredValues, double newTimeStep);// update with timestep
    void update(const Eigen::VectorXd& measuredValues,
                double newTimeStep,
                const Eigen::MatrixXd& A);// update using new timestep and dynamics

    double getCurrentStep() { return currentTime; }
    Eigen::VectorXd getState() { return xHat; }
    Eigen::MatrixXd getError() { return P; }
    Eigen::MatrixXd getInnovationError() { return innovationError; }
    double getEstimationError() { return estimationError; }
    uint64_t getTheta() { return theta; }
    float getLambda() { return lambda; }
    void setLambda(float newLambda);

  protected:
    int m, n;// system model dimensions

    /**
    * Process-specific matrices for a general KF.
    * These are using the names from the original paper.
	*   F - System transition matrix / dynamics
	*   H - Observation model matrix
	*   Q - Process noise covariance
	*   R - Measurement noise covariance
	*   P - Estimate error covariance
    *   K - Kalman gain
	*/
    Eigen::MatrixXd F, H, Q, R, P, K, P0;
    Eigen::MatrixXd I;// identity matrix, on size n

    // estimated state, estimated state +1
    Eigen::VectorXd xHat, xHatNew;
    Eigen::VectorXd innovationError;// eq. 3

    double timeStep;
    double initialTimestamp;
    double currentTime;
    double estimationError;// eq. 8

    /**
     * @brief control units for changing the new
     * frequency. Theta (θ) is static according
     * to the paper in Jain et al.
     */
    uint64_t theta = 2; // θ = 2 in all experiments
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
    CircularBuffer<long> kfErrorWindow;

    /**
     * Error calculation using the window of previous
     * values.
     * @return the current estimation error, weighted
     * by recency and size.
     */
    float totalEstimationErrorDivider;
    float calculateTotalEstimationError(); // eq. 9
    void calculateTotalEstimationErrorDivider(int size);// eq. 9 (divider, calc. once)

};// class KalmanFilter

}// namespace NES

#endif // NES_INCLUDE_UTIL_KALMAN_FILTER_HPP_
