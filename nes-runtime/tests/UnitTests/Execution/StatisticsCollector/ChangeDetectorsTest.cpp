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
#include <Execution/StatisticsCollector/ChangeDetectors/Adwin/Adwin.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/SeqDrift2/SeqDrift2.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES::Runtime::Execution {
class ChangeDetectorsTest : public testing::Test {

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("PerformanceTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup PerformanceTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup PerformanceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down PerformanceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down PerformanceTest test class." << std::endl; }

};

TEST(ChangeDetectorsTest, adwin) {
    auto adwin = Adwin(0.01, 5);
    auto tuples = 1000000;
    auto value = 0.1;

    auto runtimeStart = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < tuples; ++i) {
        adwin.insertValue(value);
    }
    auto runtimeEnd = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
    std::chrono::duration<float> secs = runtimeEnd - runtimeStart;

    std::cout << "ms " << duration.count() << std::endl;
    std::cout << "tuples/ms " << tuples/duration.count() << std::endl;

    std::cout << "sec " << secs.count() << std::endl;
    std::cout << "tuples/sec " << tuples/secs.count() << std::endl;

}

TEST(ChangeDetectorsTest, seqDrift) {
    auto seqDrift = SeqDrift2(0.01,200);
    auto tuples = 1000000;
    auto value = 0.1;

    auto runtimeStart = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < tuples; ++i) {
        seqDrift.insertValue(value);
    }
    auto runtimeEnd = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
    std::chrono::duration<float> secs = runtimeEnd - runtimeStart;

    std::cout << "ms " << duration.count() << std::endl;
    std::cout << "tuples/ms " << tuples/duration.count() << std::endl;

    std::cout << "sec " << secs.count() << std::endl;
    std::cout << "tuples/sec " << tuples/secs.count() << std::endl;

}
}