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

#include <chrono>
#include <cstdint>
#include <optional>
#include <variant>
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

TEST_F(RunPolicyTest, ARunSubmitsOnceUnlessAskedToRepeat)
{
    const SystestConfiguration config;

    const auto policy = RunPolicy::create(config);

    EXPECT_TRUE(std::holds_alternative<RunInFileOrder>(policy.ordering));
    EXPECT_TRUE(std::holds_alternative<SubmitOnce>(policy.repetition));
    EXPECT_FALSE(policy.runLimit.has_value());
    EXPECT_FALSE(policy.measureReport.has_value());
}

TEST_F(RunPolicyTest, AShuffledRunWithoutASeedDrawsOne)
{
    SystestConfiguration config;
    config.randomQueryOrder = true;

    const auto policy = RunPolicy::create(config);

    const auto* shuffled = std::get_if<RunInShuffledOrder>(&policy.ordering);
    ASSERT_NE(shuffled, nullptr);
    EXPECT_FALSE(shuffled->seed.has_value());
}

TEST_F(RunPolicyTest, AShuffledRunKeepsTheSeedItWasGiven)
{
    SystestConfiguration config;
    constexpr uint64_t seed = 42;
    config.randomQueryOrder = true;
    config.shuffleSeed = seed;

    const auto policy = RunPolicy::create(config);

    const auto* shuffled = std::get_if<RunInShuffledOrder>(&policy.ordering);
    ASSERT_NE(shuffled, nullptr);
    EXPECT_EQ(shuffled->seed, std::optional{seed});
}

TEST_F(RunPolicyTest, AnEndlessRunRepeatsUntilStopped)
{
    SystestConfiguration config;
    config.endlessMode = true;

    const auto policy = RunPolicy::create(config);

    EXPECT_TRUE(std::holds_alternative<SubmitUntilStopped>(policy.repetition));
    EXPECT_FALSE(policy.runLimit.has_value());
}

TEST_F(RunPolicyTest, EndlessRoundsBoundAnEndlessRun)
{
    SystestConfiguration config;
    config.endlessMode = true;
    config.endlessRounds = 3;

    const auto policy = RunPolicy::create(config);

    const auto* rounds = std::get_if<SubmitRounds>(&policy.repetition);
    ASSERT_NE(rounds, nullptr);
    EXPECT_EQ(rounds->count, 3);
}

TEST_F(RunPolicyTest, EndlessSecondsLimitAnEndlessRun)
{
    SystestConfiguration config;
    constexpr uint64_t seconds = 30;
    config.endlessMode = true;
    config.endlessSeconds = seconds;

    const auto policy = RunPolicy::create(config);

    EXPECT_TRUE(std::holds_alternative<SubmitUntilStopped>(policy.repetition));
    EXPECT_EQ(policy.runLimit, std::optional{std::chrono::seconds{seconds}});
}

TEST_F(RunPolicyTest, AMeasuringRunRepeatsAtLeastOnce)
{
    SystestConfiguration config;
    config.benchmark = true;
    config.benchmarkRounds = 0;

    const auto policy = RunPolicy::create(config);

    const auto* rounds = std::get_if<SubmitRounds>(&policy.repetition);
    ASSERT_NE(rounds, nullptr);
    EXPECT_EQ(rounds->count, 1);
}

TEST_F(RunPolicyTest, AMeasuringRunRepeatsItsRounds)
{
    SystestConfiguration config;
    constexpr uint64_t rounds = 5;
    config.benchmark = true;
    config.benchmarkRounds = rounds;

    const auto policy = RunPolicy::create(config);

    const auto* fixed = std::get_if<SubmitRounds>(&policy.repetition);
    ASSERT_NE(fixed, nullptr);
    EXPECT_EQ(fixed->count, rounds);
}

TEST_F(RunPolicyTest, AMeasuringRunTakesPrecedenceOverEndlessMode)
{
    SystestConfiguration config;
    constexpr uint64_t seconds = 30;
    config.benchmark = true;
    config.endlessMode = true;
    config.endlessSeconds = seconds;

    const auto policy = RunPolicy::create(config);

    EXPECT_TRUE(std::holds_alternative<SubmitRounds>(policy.repetition));
    EXPECT_FALSE(policy.runLimit.has_value());
    EXPECT_TRUE(policy.measureReport.has_value());
}

}
