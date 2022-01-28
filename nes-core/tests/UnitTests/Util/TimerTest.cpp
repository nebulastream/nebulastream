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

#include <Util/Logger.hpp>
#include <Util/Timer.hpp>
#include <chrono>
#include <gtest/gtest.h>
#include <thread>
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

/**
 * @brief Test if start and stop commands work properly.
 * @result Measured runtime is 2 sec with 0.1 sec slack
 */
TEST(UtilFunctionTest, startAndPause) {
    Timer timer = Timer("testComponent");
    timer.start();
    sleep(1);
    timer.pause();
    sleep(1);
    timer.start();
    sleep(1);
    timer.pause();

    //std::cout << timer.getRuntime();
    EXPECT_TRUE((timer.getRuntime() >= 2000000000 - 100000000) && (timer.getRuntime() <= 2000000000 + 100000000));
}

/**
 * @brief Test if snapshot taking works properly.
 * @result Measured runtime and snapshot runtime is 1 sec with 0.05 sec slack
 */
TEST(UtilFunctionTest, snapshotTaking) {
    Timer timer = Timer("testComponent");
    timer.start();
    sleep(1);
    timer.snapshot("test");
    timer.pause();

    //std::cout << timer.getRuntime();
    auto snapshots = timer.getSnapshots();
    EXPECT_TRUE((timer.getRuntime() >= 1000000000 - 50000000) && (timer.getRuntime() <= 1000000000 + 50000000));
    EXPECT_TRUE(snapshots[0].name == "testComponent_test");
    EXPECT_TRUE((snapshots[0].getRuntime() >= 1000000000 - 50000000) && (snapshots[0].getRuntime() <= 1000000000 + 50000000));
}

/**
 * @brief Test if timer merging works properly.
 * @result Measured runtime is 2 sec with 0.1 sec slack. Snapshot runtime is 1 sec with 0.05 sec slack
 */
TEST(UtilFunctionTest, mergeTimers) {
    Timer timer1 = Timer("testComponent1");
    timer1.start();
    sleep(1);
    timer1.snapshot("test1");
    timer1.pause();
    //std::cout << timer1;

    Timer timer2 = Timer("testComponent2");
    timer2.start();
    sleep(1);
    timer2.snapshot("test2");
    timer2.pause();
    timer1.merge(timer2);

    //std::cout << timer1;
    auto snapshots = timer1.getSnapshots();
    EXPECT_TRUE((timer1.getRuntime() >= 2000000000 - 100000000) && (timer1.getRuntime() <= 2000000000 + 100000000));
    EXPECT_TRUE(snapshots[0].name == "testComponent1_test1");
    EXPECT_TRUE((snapshots[0].getRuntime() >= 1000000000 - 50000000) && (snapshots[0].getRuntime() <= 1000000000 + 50000000));
    EXPECT_TRUE(snapshots[1].name == "testComponent1_testComponent2");
    EXPECT_TRUE((snapshots[1].getRuntime() >= 1000000000 - 50000000) && (snapshots[1].getRuntime() <= 1000000000 + 50000000));
    EXPECT_TRUE(snapshots[1].children[0].name == "testComponent2_test2");
    EXPECT_TRUE((snapshots[1].children[0].getRuntime() >= 1000000000 - 50000000)
                && (snapshots[1].children[0].getRuntime() <= 1000000000 + 50000000));
}

/**
 * @brief Test if sec and millisec measures work properly.
 * @result Measured runtime is 1.3003 sec with 0.05 sec slack
 */
TEST(UtilFunctionTest, differentTimeUnits) {
    Timer timer1 = Timer<std::chrono::seconds>("testComponent");
    timer1.start();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    std::this_thread::sleep_for(std::chrono::microseconds(300));
    timer1.snapshot("test");
    timer1.pause();

    Timer timer2 = Timer<std::chrono::milliseconds>("testComponent");
    timer2.start();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    std::this_thread::sleep_for(std::chrono::microseconds(300));
    timer2.snapshot("test");
    timer2.pause();

    EXPECT_TRUE(timer1.getRuntime() == 1);// runtime returned in int64_t, so expect no precision
    EXPECT_TRUE((timer2.getRuntime() >= 1300 - 50) && (timer2.getRuntime() <= 1300 + 50));
}

/**
 * @brief Test if sec measures work properly with precision of double.
 * @result Measured runtime is 1.3003 sec with 0.05 sec slack
 */
TEST(UtilFunctionTest, differentMeasurePrecision) {
    Timer timer1 = Timer<std::chrono::duration<double>, std::ratio<1>>("testComponent");
    timer1.start();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    std::this_thread::sleep_for(std::chrono::microseconds(300));
    timer1.snapshot("test");
    timer1.pause();

    // need to use print time, as runtime will round to full seconds
    EXPECT_TRUE((timer1.getPrintTime() >= 1.3 - 0.05) && (timer1.getPrintTime() <= 1.3 + 0.05));
}

/**
 * @brief Test if print conversions work properly, when set on initialization.
 * @result Measured runtime is 1.3 sec with 0.05 sec slack
 */
TEST(UtilFunctionTest, differentPrintPrecision) {
    Timer timer1 = Timer<std::chrono::seconds, std::ratio<1, 1>, int64_t>("testComponent");
    timer1.start();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    timer1.snapshot("test");
    timer1.pause();

    Timer timer2 = Timer<std::chrono::milliseconds, std::ratio<1, 1>, float>("testComponent");
    timer2.start();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    timer2.snapshot("test");
    timer2.pause();

    EXPECT_TRUE(timer1.getPrintTime() == 1);
    EXPECT_TRUE((timer2.getPrintTime() >= 1.3 - 0.05) && (timer2.getPrintTime() <= 1.3 + 0.05));
}

}// namespace NES