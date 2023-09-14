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

#include <API/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sensors/Values/SingleSensor.hpp>
#include <Util/KalmanFilter.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>
#include <ctime>

namespace NES {
KalmanFilter::KalmanFilter(const uint64_t errorWindowSize) : kfErrorWindow(errorWindowSize) {
    this->calculateTotalEstimationErrorDivider(errorWindowSize);
};

KalmanFilter::KalmanFilter(double timeStep,
                           const Eigen::MatrixXd F,
                           const Eigen::MatrixXd H,
                           const Eigen::MatrixXd Q,
                           const Eigen::MatrixXd R,
                           const Eigen::MatrixXd P,
                           const uint64_t errorWindowSize)
    : m(H.rows()), n(F.rows()), stateTransitionModel(F), observationModel(H), processNoiseCovariance(Q),
      measurementNoiseCovariance(R), estimateCovariance(P), identityMatrix(n, n), xHat(n), xHatNew(n), innovationError(n),
      valueVector(1), timeStep(timeStep), kfErrorWindow(errorWindowSize) {
    this->calculateTotalEstimationErrorDivider(errorWindowSize);
    identityMatrix.setIdentity();
}

void KalmanFilter::init() {
    this->setDefaultValues();
    this->xHat.setZero();
    this->valueVector.setZero();
}

void KalmanFilter::init(double initialTimestamp) {
    this->setDefaultValues();
    this->xHat.setZero();
    this->currentTime = initialTimestamp;
}

void KalmanFilter::init(const Eigen::VectorXd& initialState) {
    this->setDefaultValues();
    this->xHat = initialState;
}

void KalmanFilter::init(const Eigen::VectorXd& initialState, double initialTimestamp) {
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
    this->stateTransitionModel = Eigen::MatrixXd(this->n, this->n);
    this->observationModel = Eigen::MatrixXd(this->m, this->n);
    this->processNoiseCovariance = Eigen::MatrixXd(this->n, this->n);
    this->measurementNoiseCovariance = Eigen::MatrixXd(this->m, this->m);
    this->estimateCovariance = Eigen::MatrixXd(this->n, this->n);

    // Discrete LTI projectile motion, measuring position only
    this->stateTransitionModel << 1, this->timeStep, 0, 0, 1, this->timeStep, 0, 0, 1;
    this->observationModel << 1, 0, 0;

    // Reasonable covariance matrices
    this->processNoiseCovariance << .05, .05, .0, .05, .05, .0, .0, .0, .0;
    this->measurementNoiseCovariance << 5;
    this->estimateCovariance << .1, .1, .1, .1, 10000, 10, .1, 10, 100;

    // rest of initializations
    this->identityMatrix = Eigen::MatrixXd(this->n, this->n);
    this->identityMatrix.setIdentity();
    this->xHat = Eigen::VectorXd(this->n);
    this->xHatNew = Eigen::VectorXd(this->n);
    this->innovationError = Eigen::VectorXd(this->n);
    this->initialTimestamp = std::time(nullptr);
    this->currentTime = std::time(nullptr);
}

void KalmanFilter::update(const Eigen::VectorXd& measuredValues) {
    // simplified prediction phase
    xHatNew = stateTransitionModel * xHat;// no control unit (B*u), predicted a-priori state estimate
    estimateCovariance = stateTransitionModel * estimateCovariance * stateTransitionModel.transpose()
        + processNoiseCovariance;// predicted a-priori estimate covariance

    /**
     * Simplified update phase, use the
     * measured values to upate the innovation
     * error, calculate Kalman gain kalmanGain (reward)
     * and update the posteriori state
     * estimate. The updated state estimate
     * becomes the new xHat (current state).
     */
    innovationError = measuredValues - (observationModel * xHatNew);// update innovation error ψ_k, eq. 2 + 3
    kalmanGain = estimateCovariance * observationModel.transpose()
        * (observationModel * estimateCovariance * observationModel.transpose() + measurementNoiseCovariance)
              .inverse();                                                   // kalman gain
    xHatNew += kalmanGain * (measuredValues - (observationModel * xHatNew));// updated a-posteriori state estimate
    estimateCovariance =
        (identityMatrix - kalmanGain * observationModel) * estimateCovariance;// updated a-posteriori estimate covariance
    xHat = xHatNew;                                                           // updated xHat

    // update estimation error, eq.8
    this->estimationError =
        std::sqrt(((innovationError * measuredValues.inverse()) * (innovationError * measuredValues.inverse())).trace());
    this->kfErrorWindow.emplace(this->estimationError);// store result in error window
    // update timestep
    currentTime += timeStep;
}

void KalmanFilter::update(const Eigen::VectorXd& measuredValues, double newTimeStep) {
    timeStep = newTimeStep;
    this->update(measuredValues);
}

void KalmanFilter::update(const Eigen::VectorXd& measuredValues, double newTimeStep, const Eigen::MatrixXd& F) {
    this->stateTransitionModel = F;
    timeStep = newTimeStep;
    this->update(measuredValues);
}

double KalmanFilter::getTotalEstimationError() { return this->calculateTotalEstimationError(); }

float KalmanFilter::calculateTotalEstimationError() {
    float j = 1;// eq. 9 iterator
    float totalError = 0;
    for (auto errorValue : kfErrorWindow) {
        totalError += (errorValue / j);
        ++j;
    }
    return totalError / totalEstimationErrorDivider;
}

void KalmanFilter::calculateTotalEstimationErrorDivider(int size) {
    totalEstimationErrorDivider = size > 0 ? 0 : 1;
    for (int i = 1; i <= size; ++i) {
        totalEstimationErrorDivider += (1.0 / i);
    }
}

std::chrono::milliseconds KalmanFilter::getNewGatheringIntervalBaseline() {
    // eq. 10
    auto totalEstimationError = this->calculateTotalEstimationError();
    auto powerOfEuler = (totalEstimationError + lambda) / lambda;
    auto thetaPart = theta * (1 - std::pow(eulerConstant, powerOfEuler));
    auto newGatheringIntervalCandidate = this->gatheringInterval.count() + thetaPart;
    if (newGatheringIntervalCandidate >= gatheringIntervalReceived.count() - (gatheringIntervalRange.count() / 2)
        && newGatheringIntervalCandidate <= gatheringIntervalReceived.count() + (gatheringIntervalRange.count() / 2)) {// eq. 7
        // remove fractional part from double
        this->gatheringInterval = std::chrono::milliseconds((int) trunc(newGatheringIntervalCandidate));
    }
    return this->gatheringInterval;
}

std::chrono::milliseconds KalmanFilter::getNewGatheringInterval() {
    auto currentEstimationError = this->getEstimationErrorDifference();
    if (currentEstimationError > 0.6) {  // indicates immediate change
        auto frequencyCandidate = this->frequencyExponentialDecay();
        if (frequencyCandidate < this->fastestInterval.count()) {
            this->gatheringInterval = this->fastestInterval;
        } else {
            this->gatheringInterval = std::chrono::milliseconds((int) trunc(frequencyCandidate));
        }
    } else if (currentEstimationError < .24) {  // indicates consecutive similarity
        auto frequencyCandidate = this->frequencyExponentialGrowth();
        if (frequencyCandidate > this->slowestInterval.count()) {
            this->gatheringInterval = this->slowestInterval;
        } else {
            this->gatheringInterval = std::chrono::milliseconds((int) trunc(frequencyCandidate));
        }
    }
    return this->gatheringInterval;
}

void KalmanFilter::updateFromTupleBuffer(Runtime::TupleBuffer& tupleBuffer) {
    NES_TRACE("KalmanFilter::updateFromTupleBuffer: updating from a whole tuple buffer");
    this->valueVector = Eigen::VectorXd(1); // TODO: remove, only for tests that don't initialize it
    if (!!tupleBuffer) {
        auto numOfTuples = tupleBuffer.getNumberOfTuples();
        auto records = tupleBuffer.getBuffer<Sensors::SingleSensor>();
        for (uint64_t i = 0; i < numOfTuples; ++i) {
            this->valueVector << records[i].value;
            this->update(valueVector);
        }
        NES_TRACE("KalmanFilter::updateFromTupleBuffer: consumed a whole buffer");
    }
}

double KalmanFilter::getCurrentStep() { return currentTime; }
Eigen::VectorXd KalmanFilter::getState() { return xHat; }
Eigen::MatrixXd KalmanFilter::getError() { return estimateCovariance; }
Eigen::MatrixXd KalmanFilter::getInnovationError() { return innovationError; }
double KalmanFilter::getEstimationError() { return estimationError; }
double KalmanFilter::getEstimationErrorDifference() { return kfErrorWindow[0] - kfErrorWindow[1]; }
uint64_t KalmanFilter::getTheta() { return theta; }
float KalmanFilter::getLambda() { return lambda; }

void KalmanFilter::setLambda(float newLambda) { this->lambda = newLambda; }

void KalmanFilter::setGatheringInterval(std::chrono::milliseconds gatheringIntervalInMillis) {
    this->gatheringInterval = gatheringIntervalInMillis;
    this->gatheringIntervalReceived = gatheringIntervalInMillis;
    this->initialInterval = gatheringIntervalInMillis;
    this->slowestInterval = std::chrono::milliseconds((int) trunc(gatheringIntervalInMillis.count() * 1.5));
    this->fastestInterval = std::chrono::milliseconds((int) trunc(gatheringIntervalInMillis.count() * 0.5));
}

void KalmanFilter::setGatheringIntervalRange(std::chrono::milliseconds gatheringIntervalRange) {
    this->gatheringIntervalRange = gatheringIntervalRange;
}

void KalmanFilter::setGatheringIntervalWithRange(std::chrono::milliseconds gatheringIntervalInMillis,
                                                 std::chrono::milliseconds gatheringIntervalRange) {
    this->setGatheringInterval(gatheringIntervalInMillis);
    this->setGatheringIntervalRange(gatheringIntervalRange);
}

void KalmanFilter::setSlowestInterval(std::chrono::milliseconds gatheringIntervalInMillis) {
    NES_TRACE("KalmanFilter::setSlowestInterval: {}ms to {}ms", this->slowestInterval.count(), gatheringIntervalInMillis.count());
    this->slowestInterval = gatheringIntervalInMillis;
    if (this->slowestInterval < this->fastestInterval) {
        this->fastestInterval = 3 * this->slowestInterval;
    }
}

void KalmanFilter::setFastestInterval(std::chrono::milliseconds gatheringIntervalInMillis) {
    this->fastestInterval = gatheringIntervalInMillis;
}

double KalmanFilter::frequencyExponentialDecay() {
    auto newCandidate = this->gatheringInterval.count() * std::pow((1 - .25), this->decreaseCounter);
    if (std::isinf(newCandidate)) {
        return this->gatheringInterval.count();
    }
    if (decreaseCounter < this->maxValue) {
        ++this->decreaseCounter;
    }
    this->increaseCounter = 1;
    return newCandidate;
}

double KalmanFilter::frequencyExponentialGrowth() {
    auto newCandidate = this->gatheringInterval.count() * std::pow((1 + .25), this->increaseCounter);
    if (std::isinf(newCandidate)) {
        return this->gatheringInterval.count();
    }
    if (increaseCounter < this->maxValue) {
        ++this->increaseCounter;
    }
    this->decreaseCounter = 1;
    return newCandidate;
}

std::chrono::milliseconds KalmanFilter::getCurrentGatheringInterval() { return this->gatheringInterval; }

}// namespace NES