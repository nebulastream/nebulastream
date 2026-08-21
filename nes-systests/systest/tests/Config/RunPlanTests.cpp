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

#include <variant>

#include <Config/Config.hpp>
#include <Config/RunPlan.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{

class RunPlanTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("RunPlanTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup RunPlanTest test class.");
    }

    static void TearDownTestSuite() { NES_DEBUG("Tear down RunPlanTest test class."); }
};

TEST_F(RunPlanTest, APlainRunKeepsTheDiscoveredOrderAndSubmitsEveryQueryOnce)
{
    const SystestConfiguration config;

    const auto plan = RunPlan::create(config);

    EXPECT_TRUE(std::holds_alternative<SourceOrder>(plan.ordering));
    EXPECT_TRUE(std::holds_alternative<Once>(plan.repetition));
    EXPECT_FALSE(plan.measureReport.has_value());
}

TEST_F(RunPlanTest, ShufflingIsAnOrderingRatherThanAFlagTheRunChecksLater)
{
    SystestConfiguration config;
    config.randomQueryOrder = true;

    EXPECT_TRUE(std::holds_alternative<Shuffled>(RunPlan::create(config).ordering));
}

TEST_F(RunPlanTest, EndlessModeRepeatsUntilStopped)
{
    SystestConfiguration config;
    config.endlessMode = true;

    EXPECT_TRUE(std::holds_alternative<UntilStopped>(RunPlan::create(config).repetition));
}

TEST_F(RunPlanTest, AMeasuringRunNamesItsReportUnderTheWorkingDirectory)
{
    SystestConfiguration config;
    config.benchmark = true;
    config.workingDir = "/tmp/systest-run-plan-test";

    const auto plan = RunPlan::create(config);

    ASSERT_TRUE(plan.measureReport.has_value());
    EXPECT_EQ(plan.measureReport.value(), "/tmp/systest-run-plan-test/BenchmarkResults.json");
    EXPECT_TRUE(std::holds_alternative<Once>(plan.repetition));
}

TEST_F(RunPlanTest, ConcurrencyComesFromTheOptionTheCommandLineSets)
{
    SystestConfiguration config;
    config.numberConcurrentQueries = 3;

    EXPECT_EQ(RunPlan::create(config).concurrency, 3);
}

}
