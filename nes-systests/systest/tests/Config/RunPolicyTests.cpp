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

#include <Config/Config.hpp>
#include <Config/RunPolicy.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{

class RunPolicyTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("RunPolicyTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup RunPolicyTest test class.");
    }

    static void TearDownTestSuite() { NES_DEBUG("Tear down RunPolicyTest test class."); }
};

TEST_F(RunPolicyTest, AMeasuringRunNamesItsReportUnderTheWorkingDirectory)
{
    SystestConfiguration config;
    config.benchmark = true;
    config.workingDir = "/tmp/systest-run-policy-test";

    const auto policy = RunPolicy::create(config);

    ASSERT_TRUE(policy.measureReport.has_value());
    EXPECT_EQ(policy.measureReport.value(), "/tmp/systest-run-policy-test/BenchmarkResults.json");
}

TEST_F(RunPolicyTest, AMeasuringRunSubmitsOneQueryAtATime)
{
    SystestConfiguration config;
    config.benchmark = true;
    config.numberConcurrentQueries = 4;

    EXPECT_EQ(RunPolicy::create(config).concurrency, 1);
}

}
