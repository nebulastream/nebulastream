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

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Sources/AdaptiveKFSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/KalmanFilter.hpp>
#include <Util/Logger.hpp>

#include <Eigen/Dense>
#include <gtest/gtest.h>

#include <iostream>
#include <vector>

namespace NES {

class AdaptiveKFTest : public testing::Test {
  public:
    SchemaPtr schema;
    PhysicalStreamConfigPtr streamConf;
    Runtime::NodeEnginePtr nodeEngine;
    uint64_t tuple_size;
    uint64_t buffer_size;
    uint64_t num_of_buffers;
    uint64_t num_tuples_to_process;
    std::vector<double> measurements;

    static void SetUpTestCase() {
        NES::setupLogging("AdaptiveKFTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup AdaptiveKFTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down AdaptiveKFTest test class."); }

    void SetUp() override {
        NES_INFO("Setup AdaptiveKFTest class.");
        streamConf = PhysicalStreamConfig::createEmpty();
        schema = Schema::create()->addField("temperature", UINT32);
        nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1", 31337, streamConf, 1, 4096, 1024, 12, 12);
        tuple_size = schema->getSchemaSizeInBytes();
        buffer_size = nodeEngine->getBufferManager()->getBufferSize();
        num_of_buffers = 1;
        num_tuples_to_process = num_of_buffers * (buffer_size / tuple_size);

        const DataSourcePtr source =
            createAdaptiveKFSource(schema, nodeEngine->getBufferManager(),
                                   nodeEngine->getQueryManager(), num_tuples_to_process,
                                   num_of_buffers, 1, 12, 1);

        // Fake measurements for y with noise
        measurements = {
            1.04202710058, 1.10726790452, 1.2913511148, 1.48485250951, 1.72825901034,
            1.74216489744, 2.11672039768, 2.14529225112, 2.16029641405, 2.21269371128,
            2.57709350237, 2.6682215744, 2.51641839428, 2.76034056782, 2.88131780617,
            2.88373786518, 2.9448468727, 2.82866600131, 3.0006601946, 3.12920591669,
            2.858361783, 2.83808170354, 2.68975330958, 2.66533185589, 2.81613499531,
            2.81003612051, 2.88321849354, 2.69789264832, 2.4342229249, 2.23464791825,
            2.30278776224, 2.02069770395, 1.94393985809, 1.82498398739, 1.52526230354,
            1.86967808173, 1.18073207847, 1.10729605087, 0.916168349913, 0.678547664519,
            0.562381751596, 0.355468474885, -0.155607486619, -0.287198661013, -0.602973173813
        };
    }

    void TearDown() override { NES_INFO("Tear down AdaptiveKFTest class."); }
};

TEST_F(AdaptiveKFTest, kfErrorChangeTest) {

    // time and step size
    double initialTimestamp = 0;
    double timeStep = 1.0/30;

    // measurements
    int m = 1;
    // states
    int n = 3;

    // initial values of matrices
    Eigen::MatrixXd F(n, n); // system dynamics
    Eigen::MatrixXd H(m, n); // observation model
    Eigen::MatrixXd Q(n, n); // process noise covariance
    Eigen::MatrixXd R(m, m); // measurement noise covariance
    Eigen::MatrixXd P0(n, n); // estimate error covariance

    // initial state estimations, values can be anything
    Eigen::VectorXd initialState(n);

    // Discrete LTI projectile motion, measuring position only
    F << 1, timeStep, 0, 0, 1, timeStep, 0, 0, 1;
    H << 1, 0, 0;

    // Reasonable covariance matrices
    Q << .05, .05, .0, .05, .05, .0, .0, .0, .0;
    R << 5;
    P0 << .1, .1, .1, .1, 10000, 10, .1, 10, 100;

    // Construct the filter
    KalmanFilter kalmanFilter(timeStep, F, H, Q, R, P0);

    initialState << 0, measurements[0], -9.81;
    kalmanFilter.init(initialTimestamp, initialState);

    // start measurements vector
    Eigen::VectorXd y(m);

    // predict and update
    for(auto measurement : measurements) {
        initialTimestamp += timeStep;
        y << measurement;
        kalmanFilter.update(y);
    }

    // error has changed and P != P0
    EXPECT_NE(P0, kalmanFilter.getError());
}

TEST_F(AdaptiveKFTest, kfStateChangeTest) {

    // time and step size
    double initialTimestamp = 0;
    double timeStep = 1.0/30;

    // measurements
    int m = 1;
    // states
    int n = 3;

    // initial values of matrices
    Eigen::MatrixXd F(n, n); // system dynamics
    Eigen::MatrixXd H(m, n); // observation model
    Eigen::MatrixXd Q(n, n); // process noise covariance
    Eigen::MatrixXd R(m, m); // measurement noise covariance
    Eigen::MatrixXd P0(n, n); // estimate error covariance

    // initial state estimations, values can be anything
    Eigen::VectorXd initialState(n);

    // Discrete LTI projectile motion, measuring position only
    F << 1, timeStep, 0, 0, 1, timeStep, 0, 0, 1;
    H << 1, 0, 0;

    // Reasonable covariance matrices
    Q << .05, .05, .0, .05, .05, .0, .0, .0, .0;
    R << 5;
    P0 << .1, .1, .1, .1, 10000, 10, .1, 10, 100;

    // Construct the filter
    KalmanFilter kalmanFilter(timeStep, F, H, Q, R, P0);

    initialState << 0, measurements[0], -9.81;
    kalmanFilter.init(initialTimestamp, initialState);

    // start measurements vector
    Eigen::VectorXd y(m);

    // predict and update
    for(auto measurement : measurements) {
        initialTimestamp += timeStep;
        y << measurement;

        // get current xHat, update, assert NE with new one
        auto oldXHat = kalmanFilter.getState();
        kalmanFilter.update(y);
        EXPECT_NE(oldXHat, kalmanFilter.getState());
    }
}

TEST_F(AdaptiveKFTest, kfStepChangeTest) {

    // time and step size
    double initialTimestamp = 0;
    double timeStep = 1.0/30;

    // measurements
    int m = 1;
    // states
    int n = 3;

    // initial values of matrices
    Eigen::MatrixXd F(n, n); // system dynamics
    Eigen::MatrixXd H(m, n); // observation model
    Eigen::MatrixXd Q(n, n); // process noise covariance
    Eigen::MatrixXd R(m, m); // measurement noise covariance
    Eigen::MatrixXd P0(n, n); // estimate error covariance

    // initial state estimations, values can be anything
    Eigen::VectorXd initialState(n);

    // Discrete LTI projectile motion, measuring position only
    F << 1, timeStep, 0, 0, 1, timeStep, 0, 0, 1;
    H << 1, 0, 0;

    // Reasonable covariance matrices
    Q << .05, .05, .0, .05, .05, .0, .0, .0, .0;
    R << 5;
    P0 << .1, .1, .1, .1, 10000, 10, .1, 10, 100;

    // Construct the filter
    KalmanFilter kalmanFilter(timeStep, F, H, Q, R, P0);

    initialState << 0, measurements[0], -9.81;
    kalmanFilter.init(initialTimestamp, initialState);

    // start measurements vector
    Eigen::VectorXd y(m);

    // predict and update
    for(auto measurement : measurements) {
        initialTimestamp += timeStep;
        y << measurement;

        // get current step, update, assert NE with new one
        auto oldStep = kalmanFilter.getCurrentStep();
        kalmanFilter.update(y);
        EXPECT_NE(oldStep, kalmanFilter.getCurrentStep());
        // step increased by the pre-defined interval only
        EXPECT_EQ(oldStep + timeStep, kalmanFilter.getCurrentStep());
    }
}

TEST_F(AdaptiveKFTest, kfInnovationErrorChangeTest) {

    // time and step size
    double initialTimestamp = 0;
    double timeStep = 1.0/30;

    // measurements
    int m = 1;
    // states
    int n = 3;

    // initial values of matrices
    Eigen::MatrixXd F(n, n); // system dynamics
    Eigen::MatrixXd H(m, n); // observation model
    Eigen::MatrixXd Q(n, n); // process noise covariance
    Eigen::MatrixXd R(m, m); // measurement noise covariance
    Eigen::MatrixXd P0(n, n); // estimate error covariance

    // initial state estimations, values can be anything
    Eigen::VectorXd initialState(n);

    // Discrete LTI projectile motion, measuring position only
    F << 1, timeStep, 0, 0, 1, timeStep, 0, 0, 1;
    H << 1, 0, 0;

    // Reasonable covariance matrices
    Q << .05, .05, .0, .05, .05, .0, .0, .0, .0;
    R << 5;
    P0 << .1, .1, .1, .1, 10000, 10, .1, 10, 100;

    // Construct the filter
    KalmanFilter kalmanFilter(timeStep, F, H, Q, R, P0);

    initialState << 0, measurements[0], -9.81;
    kalmanFilter.init(initialTimestamp, initialState);

    // start measurements vector
    Eigen::VectorXd y(m);

    // predict and update
    for(auto measurement : measurements) {
        initialTimestamp += timeStep;
        y << measurement;

        // get current error, update, assert NE with new one
        // innovation error is a column matrix, m*1 dimensions
        auto oldInnovError = kalmanFilter.getInnovationError();
        kalmanFilter.update(y);
        auto newInnovError = kalmanFilter.getInnovationError();
        ASSERT_TRUE(oldInnovError(0) != newInnovError(0));
    }
}
}// namespace NES