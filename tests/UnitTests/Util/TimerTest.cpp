/*
    Copyright (C) 2021 by the NebulaStream project (https://nebula.stream)

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

#include <Util/Logger.hpp>
#include <Util/Timer.hpp>
#include <gtest/gtest.h>
#include <unistd.h>

namespace NES {
class TimerTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("TimerTest.log", NES::LOG_DEBUG);

        NES_INFO("TimerTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() { NES_INFO("TimerTest test class TearDownTestCase."); }
};

TEST(UtilFunctionTest, startAndPause) {
    Timer timer = Timer("testComponent");
    timer.start();
    sleep(1);
    timer.pause();

    //std::cout << timer.getRuntime();
    EXPECT_TRUE((timer.getRuntime() >= 1000000000 - 10000000) && (timer.getRuntime() <= 1000000000 + 10000000));
}

TEST(UtilFunctionTest, startAndPause2) {
    Timer timer = Timer("testComponent");
    timer.start();
    sleep(1);
    timer.pause();
    sleep(1);
    timer.start();
    sleep(1);
    timer.pause();

    //std::cout << timer.getRuntime();
    EXPECT_TRUE((timer.getRuntime() >= 2000000000 - 10000000) && (timer.getRuntime() <= 2000000000 + 10000000));
}

TEST(UtilFunctionTest, snapshotTaking) {
    Timer timer = Timer("testComponent");
    timer.start();
    sleep(1);
    timer.snapshot("test");
    timer.pause();

    //std::cout << timer.getRuntime();
    EXPECT_TRUE((timer.getRuntime() >= 1000000000 - 10000000) && (timer.getRuntime() <= 1000000000 + 10000000));
}

TEST(UtilFunctionTest, mergeTimers) {
    Timer timer1 = Timer("testComponent1");
    timer1.start();
    sleep(1);
    timer1.snapshot("test1");
    timer1.pause();
    std::cout << timer1;

    Timer timer2 = Timer("testComponent2");
    timer2.start();
    sleep(1);
    timer2.snapshot("test2");
    timer2.pause();
    timer1.merge(timer2);
    auto snapshots = timer1.getSnapshots();

    //std::cout << timer1;
    EXPECT_TRUE((timer1.getRuntime() >= 2000000000 - 10000000) && (timer1.getRuntime() <= 2000000000 + 10000000));
    EXPECT_TRUE(snapshots[0].first == "testComponent1_test1");
    EXPECT_TRUE((snapshots[0].second.count() >= 1000000000 - 10000000) && (snapshots[0].second.count() <= 1000000000 + 10000000));
    EXPECT_TRUE(snapshots[1].first == "testComponent2_test2");
    EXPECT_TRUE((snapshots[1].second.count() >= 1000000000 - 10000000) && (snapshots[1].second.count() <= 1000000000 + 10000000));
}
}