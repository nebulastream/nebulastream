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

#include <Util/KalmanFilter.hpp>

namespace NES {
KalmanFilter::KalmanFilter(){};

KalmanFilter::KalmanFilter(double timeStep, const Eigen::MatrixXd F, const Eigen::MatrixXd H, const Eigen::MatrixXd Q,
                                                   const Eigen::MatrixXd R, const Eigen::MatrixXd P)
    : F(F), H(H), Q(Q), R(R), P0(P), m(H.rows()), n(F.rows()), timeStep(timeStep), initialized(false),
      I(n, n), x_hat(n), x_hat_new(n) { I.setIdentity(); }

void KalmanFilter::init() {
    x_hat.setZero();
    P = P0;
    initialTimestamp = 0;
    currentTime = 0;
    initialized = true;
}

void KalmanFilter::init(double firstTimestamp, const Eigen::VectorXd& initialState) {
    x_hat = initialState;
    P = P0;
    initialTimestamp = firstTimestamp;
    currentTime = firstTimestamp;
    initialized = true;
}

void KalmanFilter::update(const Eigen::VectorXd& measuredValues) {
    if (!initialized) {
        this->init();
    }

    // simplified prediction phase
    x_hat_new = F * x_hat; // no control unit (B*u), predicted a-priori state estimate
    P = F * P * F.transpose() + Q; // predicted a-priori estimate covariance

    // simplified update phase
    K = P * H.transpose() * (H * P * H.transpose() + R).inverse(); // kalman gain
    x_hat_new += K * (measuredValues - H * x_hat_new);// updated a-posteriori state estimate
    P = (I - K * H) * P; // updated a-posteriori estimate covariance
    x_hat = x_hat_new; // updated x_hat

    currentTime += timeStep;
}

void KalmanFilter::update(const Eigen::VectorXd& measuredValues, double newTimeStep) {
    timeStep = newTimeStep;
    this->update(measuredValues);
}

void KalmanFilter::update(const Eigen::VectorXd& measuredValues, double newTimeStep, const Eigen::MatrixXd& F) {
    this->F = F;
    timeStep = newTimeStep;
    this->update(measuredValues);
}

}// namespace NES