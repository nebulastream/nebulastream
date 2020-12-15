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

KalmanFilter::KalmanFilter(double timeStep, const Eigen::MatrixXd A, const Eigen::MatrixXd C, const Eigen::MatrixXd Q,
                                                   const Eigen::MatrixXd R, const Eigen::MatrixXd P)
    : A(A), C(C), Q(Q), R(R), P0(P), m(C.rows()), n(A.rows()), timeStep(timeStep), initialized(false),
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

    x_hat_new = A * x_hat;
    P = A * P * A.transpose() + Q;
    K = P * C.transpose() * (C * P * C.transpose() + R).inverse();
    x_hat_new += K * (measuredValues - C * x_hat_new);
    P = (I - K * C) * P;
    x_hat = x_hat_new;

    currentTime += timeStep;
}

void KalmanFilter::update(const Eigen::VectorXd& measuredValues, double newTimeStep, const Eigen::MatrixXd& A) {
    this->A = A;
    timeStep = newTimeStep;
    this->update(measuredValues);
}

}// namespace NES