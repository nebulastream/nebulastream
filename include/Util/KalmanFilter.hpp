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

#ifndef INCLUDE_KALMANFILTER_H_
#define INCLUDE_KALMANFILTER_H_

#include <Eigen/Dense>

namespace NES {

class KalmanFilter {

  public:
    explicit KalmanFilter(double timeStep,
                          Eigen::MatrixXd F,
                          Eigen::MatrixXd H,
                          Eigen::MatrixXd Q,
                          Eigen::MatrixXd R,
                          Eigen::MatrixXd P);
    KalmanFilter();

    void init();// all zeroes
    void init(const Eigen::VectorXd& initialState);
    void init(double initialTimestamp, const Eigen::VectorXd& initialState);
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

  private:
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

};// class KalmanFilter

}// namespace NES

#endif /* INCLUDE_KALMANFILTER_H_ */
