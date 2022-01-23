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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Util/KalmanFilter.hpp>
#include <Util/Logger.hpp>

#include <Eigen/Dense>
#include <gtest/gtest.h>

#include <iostream>
#include <filesystem>
#include <fstream>
#include <vector>

namespace NES {

class AdaptiveKFTest : public testing::Test {
  public:
    SchemaPtr schema;
    PhysicalSourcePtr streamConf;
    Runtime::NodeEnginePtr nodeEngine;
    std::vector<double> measurements;
    float defaultEstimationErrorDivider = 2.9289684;
    std::chrono::time_point<std::chrono::system_clock,std::chrono::milliseconds> now_ms;

    static void SetUpTestCase() {
        NES::setupLogging("AdaptiveKFTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup AdaptiveKFTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down AdaptiveKFTest test class."); }

    void SetUp() override {
        NES_INFO("Setup AdaptiveKFTest class.");
        streamConf = PhysicalSource::create("x","x1");
        schema = Schema::create()->addField("temperature", UINT32);
        nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1", 31337, {streamConf}, 1, 4096, 1024, 12, 12);
        now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
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
            0.562381751596, 0.355468474885, 500, 900, 900,
            -0.155607486619, -0.287198661013, -0.602973173813
        };
    }

    void TearDown() override { NES_INFO("Tear down AdaptiveKFTest class."); }

    std::string getTsInRfc3339(std::chrono::milliseconds gatheringInterval = std::chrono::milliseconds{0}) {
        if (gatheringInterval.count() > 0) {
            now_ms += std::chrono::milliseconds(gatheringInterval.count());
        }
        const auto now_s = std::chrono::time_point_cast<std::chrono::seconds>(now_ms);
        const auto millis = now_ms - now_s;
        const auto c_now = std::chrono::system_clock::to_time_t(now_s);

        std::stringstream ss;
        ss << std::put_time(gmtime(&c_now), "%FT%T") << '.' << std::setfill('0') << std::setw(3) << millis.count() << 'Z';
        return ss.str();
    }
};

class KFProxy : public KalmanFilter {
  public:
    KFProxy() : KalmanFilter(){};
    KFProxy(uint64_t errorWindowSize) : KalmanFilter(errorWindowSize){};

  private:
    FRIEND_TEST(AdaptiveKFTest, kfUpdateNoTimestepTest);
    FRIEND_TEST(AdaptiveKFTest, kfUpdateWithTimestepTest);
    FRIEND_TEST(AdaptiveKFTest, kfErrorDividerDefaultSizeTest);
    FRIEND_TEST(AdaptiveKFTest, kfErrorDividerCustomSizeTest);
    FRIEND_TEST(AdaptiveKFTest, kfEstimationErrorEmptyWindowTest);
    FRIEND_TEST(AdaptiveKFTest, kfEstimationErrorFilledWindowTest);
    FRIEND_TEST(AdaptiveKFTest, kfErrorDividerTest);
    FRIEND_TEST(AdaptiveKFTest, kfNewGatheringIntervalMillisTest);
};

TEST_F(AdaptiveKFTest, kfErrorChangeTest) {

    // empty filter
    KalmanFilter kalmanFilter;
    kalmanFilter.setDefaultValues();

    // initial state estimations, values can be random
    Eigen::VectorXd initialState(3);
    initialState << 0, measurements[0], -9.81;
    kalmanFilter.init(initialState);

    // start measurements vector
    Eigen::VectorXd y(1);

    auto initialError = kalmanFilter.getError();

    // predict and update
    for(auto measurement : measurements) {
        y << measurement;
        kalmanFilter.update(y);
    }

    // error has changed and estimateCovariance != initialEstimateCovariance
    EXPECT_NE(initialError, kalmanFilter.getError());
}

TEST_F(AdaptiveKFTest, kfStateChangeTest) {

    // empty filter
    KalmanFilter kalmanFilter;
    kalmanFilter.setDefaultValues();

    // initial state estimations, values can be random
    Eigen::VectorXd initialState(3);
    initialState << 0, measurements[0], -9.81;
    kalmanFilter.init(initialState);

    // start measurements vector
    Eigen::VectorXd y(1);

    auto initialError = kalmanFilter.getError();

    // predict and update
    for(auto measurement : measurements) {
        y << measurement;

        // get current xHat, update, assert NE with new one
        auto oldXHat = kalmanFilter.getState();
        kalmanFilter.update(y);
        ASSERT_NE(oldXHat, kalmanFilter.getState());
    }
}

TEST_F(AdaptiveKFTest, kfStateChangeEmptyInitialStateTest) {

    // empty filter
    KalmanFilter kalmanFilter;
    kalmanFilter.setDefaultValues();

    // empty initial state, should be all zeroes
    kalmanFilter.init();

    // start measurements vector
    Eigen::VectorXd y(1);

    auto initialError = kalmanFilter.getError();

    // predict and update
    for(auto measurement : measurements) {
        y << measurement;

        // get current xHat, update, assert NE with new one
        auto oldXHat = kalmanFilter.getState();
        kalmanFilter.update(y);
        ASSERT_NE(oldXHat, kalmanFilter.getState());
    }
}

TEST_F(AdaptiveKFTest, kfStepChangeTest) {

    // empty filter
    KalmanFilter kalmanFilter;
    kalmanFilter.setDefaultValues();

    // initial state estimations, values can be random
    Eigen::VectorXd initialState(3);
    initialState << 0, measurements[0], -9.81;
    kalmanFilter.init(initialState);

    // start measurements vector
    Eigen::VectorXd y(1);

    auto initialError = kalmanFilter.getError();

    // predict and update
    for(auto measurement : measurements) {
        y << measurement;

        // get current step, update, assert NE with new one
        auto oldStep = kalmanFilter.getCurrentStep();
        kalmanFilter.update(y);
        ASSERT_NE(oldStep, kalmanFilter.getCurrentStep());
    }
}

TEST_F(AdaptiveKFTest, kfInnovationErrorChangeTest) {

    // empty filter
    KalmanFilter kalmanFilter;
    kalmanFilter.setDefaultValues();

    // initial state estimations, values can be random
    Eigen::VectorXd initialState(3);
    initialState << 0, measurements[0], -9.81;
    kalmanFilter.init(initialState);

    // start measurements vector
    Eigen::VectorXd y(1);

    auto initialError = kalmanFilter.getError();

    // predict and update
    for(auto measurement : measurements) {
        y << measurement;

        // get current error, update, assert NE with new one
        // innovation error is a column matrix, m*1 dimensions
        auto oldInnovError = kalmanFilter.getInnovationError();
        kalmanFilter.update(y);
        auto newInnovError = kalmanFilter.getInnovationError();
        ASSERT_NE(oldInnovError(0), newInnovError(0));
    }
}

TEST_F(AdaptiveKFTest, kfLambdaChangeTest) {

    // empty filter
    KalmanFilter kalmanFilter;
    kalmanFilter.setDefaultValues();

    // initial state estimations, values can be random
    Eigen::VectorXd initialState(3);
    initialState << 0, measurements[0], -9.81;
    kalmanFilter.init(initialState);

    auto oldLambda = kalmanFilter.getLambda();
    kalmanFilter.setLambda(0.1);
    EXPECT_NE(oldLambda, kalmanFilter.getLambda());
}

TEST_F(AdaptiveKFTest, kfUpdateNoTimestepTest) {
    // empty filter
    KFProxy kfProxy;
    kfProxy.setDefaultValues();

    // initial state estimations, values can be random
    Eigen::VectorXd initialState(3);
    initialState << 0, measurements[0], -9.81;
    kfProxy.init(initialState);

    // start measurements vector
    Eigen::VectorXd y(1);
    auto oldStep = kfProxy.getCurrentStep();
    EXPECT_EQ(oldStep, kfProxy.initialTimestamp);
    kfProxy.update(y);
    auto newStep = kfProxy.getCurrentStep();
    EXPECT_NE(newStep, kfProxy.initialTimestamp);
    EXPECT_NEAR(newStep - oldStep, kfProxy.timeStep, 0.01);
}

TEST_F(AdaptiveKFTest, kfUpdateWithTimestepTest) {
    // empty filter
    KFProxy kfProxy;
    kfProxy.setDefaultValues();
    auto initialTs = kfProxy.initialTimestamp;

    // initial state estimations, values can be random
    Eigen::VectorXd initialState(3);
    sleep(1);
    auto newTs = std::time(nullptr);
    kfProxy.init(initialState, newTs);

    EXPECT_NE(initialTs, kfProxy.initialTimestamp);
    EXPECT_EQ(kfProxy.initialTimestamp, newTs);
    EXPECT_EQ(kfProxy.currentTime, kfProxy.initialTimestamp);
}

TEST_F(AdaptiveKFTest, kfErrorDividerTest) {
    // window of 2
    KFProxy kfProxy{2};
    kfProxy.setDefaultValues();
    auto errorDivider = kfProxy.totalEstimationErrorDivider;
    EXPECT_NE(errorDivider, 0);
    EXPECT_NEAR(errorDivider, 1.5, 0.01);

    // 10, then 20, error should be (20/1 + 10/2) / (1/1 + 1/2) = 16.667
    kfProxy.kfErrorWindow.push(10);
    kfProxy.kfErrorWindow.push(20);
    auto totalEstError = kfProxy.calculateTotalEstimationError();
    EXPECT_NEAR(totalEstError, 16.66, 0.01);
}

TEST_F(AdaptiveKFTest, kfErrorDividerDefaultSizeTest) {
    // default filter
    KFProxy kfProxy;
    kfProxy.setDefaultValues();
    auto errorDivider = kfProxy.totalEstimationErrorDivider;
    EXPECT_NE(errorDivider, 0);
    EXPECT_NEAR(errorDivider, this->defaultEstimationErrorDivider, 0.01);
}

TEST_F(AdaptiveKFTest, kfErrorDividerCustomSizeTest) {
    // window of 1
    KFProxy kfProxy{1};
    kfProxy.setDefaultValues();
    auto errorDivider = kfProxy.totalEstimationErrorDivider;
    EXPECT_NE(errorDivider, 0);
    EXPECT_NEAR(errorDivider, 1, 0.01);

    // window of 0
    KFProxy kfProxy1{0};
    kfProxy1.setDefaultValues();
    errorDivider = kfProxy1.totalEstimationErrorDivider;
    EXPECT_EQ(errorDivider, 1);

    // window of 50
    KFProxy kfProxy2{50};
    kfProxy2.setDefaultValues();
    errorDivider = kfProxy2.totalEstimationErrorDivider;
    EXPECT_NE(errorDivider, 0);
    EXPECT_NEAR(errorDivider, 4.5, 0.1);
}

TEST_F(AdaptiveKFTest, kfEstimationErrorEmptyWindowTest) {
    // default filter
    KFProxy kfProxy{0};
    kfProxy.setDefaultValues();
    EXPECT_EQ(kfProxy.calculateTotalEstimationError(), 0);

    // window of 1
    KFProxy kfProxy1{1};
    kfProxy1.setDefaultValues();
    EXPECT_EQ(kfProxy1.calculateTotalEstimationError(), 0);

    // window of 50
    KFProxy kfProxy2{50};
    kfProxy2.setDefaultValues();
    EXPECT_EQ(kfProxy2.calculateTotalEstimationError(), 0);
}

TEST_F(AdaptiveKFTest, kfEstimationErrorFilledWindowTest) {
    // default filter w/ window of 1
    KFProxy kfProxy{1};
    kfProxy.setDefaultValues();
    kfProxy.kfErrorWindow.push(2);
    EXPECT_EQ(kfProxy.calculateTotalEstimationError(), 2);
    kfProxy.kfErrorWindow.push(0); // will replace 2
    EXPECT_EQ(kfProxy.calculateTotalEstimationError(), 0);

    // window of 2
    KFProxy kfProxy2{2};
    kfProxy2.setDefaultValues();
    long value = 0;
    while (!kfProxy2.kfErrorWindow.full()) {
        kfProxy2.kfErrorWindow.push(value);
        value++;
    }
    EXPECT_NE(kfProxy2.calculateTotalEstimationError(), 0);
    EXPECT_NEAR(kfProxy2.calculateTotalEstimationError(), 0.6, 0.1);
}

TEST_F(AdaptiveKFTest, kfSimpleInitTest) {
    // default filter w/ window of 1
    KFProxy kfProxy;
    kfProxy.init();

    // initial values are zero and size is defaulted to 3 in init
    EXPECT_EQ(kfProxy.getState().size(), 3);
    for (int i = 0; i < kfProxy.getState().size(); ++i) {
        EXPECT_EQ(kfProxy.getState()[i], 0);
    }
}

TEST_F(AdaptiveKFTest, kfInitWithStateTest) {
    // initial state estimations, values can be random
    Eigen::VectorXd initialState(2);
    initialState << 0, measurements[0];

    // default filter w/ window of 1
    KFProxy kfProxy;
    kfProxy.init(initialState);

    // initial values from previous step and size is 3
    EXPECT_EQ(kfProxy.getState().size(), 2);
    EXPECT_EQ(kfProxy.getState()[0], 0);
    EXPECT_EQ(kfProxy.getState()[1], measurements[0]);
}

TEST_F(AdaptiveKFTest, kfNewGatheringIntervalMillisTest) {
    // initial state estimations, values can be random
    Eigen::VectorXd initialState(3);
    initialState << 0, measurements[0], -9.81;

    // empty filter
    KFProxy kfProxy{2};
    kfProxy.init(initialState);
    auto oldFrequency = kfProxy.frequency;

    kfProxy.kfErrorWindow.push(0);
    kfProxy.kfErrorWindow.push(1);
    EXPECT_NE(kfProxy.calculateTotalEstimationError(), 0);
    EXPECT_NEAR(kfProxy.calculateTotalEstimationError(), 0.6, 0.1);

    auto newFrequency = kfProxy.getNewFrequency();
    EXPECT_NE(oldFrequency.count(), newFrequency.count());
    EXPECT_GT(oldFrequency.count(), newFrequency.count());
}

TEST_F(AdaptiveKFTest, kfUpdateUnusualValueTest) {
    // update two filters once with the same value
    // compare errors after one "normal" and one "abnormal" value

    // keep last 2 error values
    KalmanFilter kfNormal{2}; // normal
    KalmanFilter kfAbnormal{2};// abnormal

    // initial state estimations, values are the same up to this point
    Eigen::VectorXd initialState(3);
    initialState << 0, measurements[0], measurements[1];
    kfNormal.init(initialState);
    kfAbnormal.init(initialState);
    EXPECT_EQ(initialState, kfNormal.getState());
    EXPECT_EQ(initialState, kfAbnormal.getState());
    EXPECT_EQ(kfNormal.getState(), kfAbnormal.getState());

    // start measurements vector
    Eigen::VectorXd y(1);
    y << measurements[2]; // 1.2913511148
    kfNormal.update(y); // update once, this has a huge error due to starting vals
    kfAbnormal.update(y);
    EXPECT_EQ(kfNormal.getEstimationError(), kfAbnormal.getEstimationError());

    // update 1st filter, normal value (similar to first update)
    y << measurements[3]; // 1.48485250951, somewhat "normal"
    kfNormal.update(y);
    auto normalEstimationError = kfNormal.getEstimationError();

    // update 2nd filter, abnormal value (very different from first update)
    y << -0.287; // didn't expect this, chief!
    kfAbnormal.update(y);
    auto abnormalEstimationError = kfAbnormal.getEstimationError();

    // assert that error is bigger when value is "abnormal"
    EXPECT_NE(abnormalEstimationError, normalEstimationError);
    EXPECT_GT(abnormalEstimationError, normalEstimationError);
}
}// namespace NES