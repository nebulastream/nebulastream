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
#include <ctime>

namespace NES {
KalmanFilter::KalmanFilter(){};

KalmanFilter::KalmanFilter(double timeStep,
                           const Eigen::MatrixXd F,
                           const Eigen::MatrixXd H,
                           const Eigen::MatrixXd Q,
                           const Eigen::MatrixXd R,
                           const Eigen::MatrixXd P)
    : m(H.rows()), n(F.rows()), F(F), H(H), Q(Q), R(R), P0(P), I(n, n), xHat(n), xHatNew(n), innovationError(n),
      timeStep(timeStep) {
    I.setIdentity();
}

void KalmanFilter::init() {
    this->setDefaultValues();
    this->xHat.setZero();
}

void KalmanFilter::init(const Eigen::VectorXd& initialState) {
    this->setDefaultValues();
    this->xHat = initialState;
}

void KalmanFilter::init(double initialTimestamp, const Eigen::VectorXd& initialState) {
    this->setDefaultValues();
    this->xHat = initialState;
    this->initialTimestamp = initialTimestamp;
    this->currentTime = initialTimestamp;
}

void KalmanFilter::setDefaultValues() {

    // measurements
    this->m = 1;
    // states
    this->n = 3;
    // timestep value
    this->timeStep = 1.0 / 30;

    // initialize system dymanics and observation matrices
    this->F = Eigen::MatrixXd(this->n, this->n);
    this->H = Eigen::MatrixXd(this->m, this->n);
    this->Q = Eigen::MatrixXd(this->n, this->n);
    this->R = Eigen::MatrixXd(this->m, this->m);
    this->P0 = Eigen::MatrixXd(this->n, this->n);

    // Discrete LTI projectile motion, measuring position only
    this->F << 1, this->timeStep, 0, 0, 1, this->timeStep, 0, 0, 1;
    this->H << 1, 0, 0;

    // Reasonable covariance matrices
    this->Q << .05, .05, .0, .05, .05, .0, .0, .0, .0;
    this->R << 5;
    this->P0 << .1, .1, .1, .1, 10000, 10, .1, 10, 100;

    // rest of initializations
    this->P = this->P0;
    this->I = Eigen::MatrixXd(this->n, this->n);
    this->I.setIdentity();
    this->xHat = Eigen::VectorXd(this->n);
    this->xHatNew = Eigen::VectorXd(this->n);
    this->innovationError = Eigen::VectorXd(this->n);
    this->initialTimestamp = std::time(nullptr);
    this->currentTime = std::time(nullptr);
}

void KalmanFilter::update(const Eigen::VectorXd& measuredValues) {
    // simplified prediction phase
    xHatNew = F * xHat;           // no control unit (B*u), predicted a-priori state estimate
    P = F * P * F.transpose() + Q;// predicted a-priori estimate covariance

    // simplified update phase
    innovationError = measuredValues - (H * xHatNew);             // update innovation error ψ_k, eq. 2 + 3
    K = P * H.transpose() * (H * P * H.transpose() + R).inverse();// kalman gain
    xHatNew += K * (measuredValues - (H * xHatNew));              // updated a-posteriori state estimate
    P = (I - K * H) * P;                                          // updated a-posteriori estimate covariance
    xHat = xHatNew;                                               // updated xHat

    // update estimation error, eq.8
    this->estimationError =
        std::sqrt(((innovationError * measuredValues.inverse()) * (innovationError * measuredValues.inverse())).trace());

    // update timestep
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