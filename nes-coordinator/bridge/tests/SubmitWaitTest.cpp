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
#include <future>
#include <string>
#include <string_view>
#include <thread>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <nes-coordinator-bridge/coordinator.h>
#include <rust/cxx.h>
#include <BaseUnitTest.hpp>
#include <BridgeError.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

constexpr std::string_view createSource = "CREATE LOGICAL SOURCE stream (id UINT64 NOT NULL)";

Bridge::StatementOutcome submit(const Bridge::Coordinator& coordinator, const std::string_view sql, const Bridge::WaitMode wait)
{
    return coordinator.submit(rust::Str{sql.data(), sql.size()}, wait, 0, false);
}

}

class SubmitWaitTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SubmitWaitTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

/// Only a query can be waited on. Any other statement is applied and returns at once.
TEST_F(SubmitWaitTest, AWaitOnAStatementThatCreatesNoQueryReturnsAtOnce)
{
    const auto coordinator
        = Bridge::start_coordinator(Bridge::StateBackendKind::Memory, rust::Str{}, Bridge::WorkerMode::Remote, rust::Str{});

    const auto outcome = submit(*coordinator, createSource, Bridge::WaitMode::UntilCompleted);

    EXPECT_TRUE(Bridge::isOk(outcome.error)) << std::string_view{outcome.error.msg.data(), outcome.error.msg.size()};
    EXPECT_EQ(outcome.query_id, 0);
}

/// A timed out query is still running, and the caller needs its id to stop it.
/// No worker runs at the address, so the query never completes.
TEST_F(SubmitWaitTest, ATimedOutWaitKeepsTheIdOfItsQuery)
{
    const auto coordinator
        = Bridge::start_coordinator(Bridge::StateBackendKind::Memory, rust::Str{}, Bridge::WorkerMode::Remote, rust::Str{});
    for (const std::string_view setup :
         {"CREATE WORKER 'worker:8080' SET ('worker:9090' AS DATA)",
          "CREATE LOGICAL SOURCE input (id UINT64 NOT NULL)",
          R"(CREATE PHYSICAL SOURCE FOR input TYPE File SET (
              'input.csv' AS "SOURCE".FILE_PATH, 'worker:8080' AS "SOURCE".HOST, 'CSV' AS INPUT_FORMATTER."TYPE"))",
          R"(CREATE SINK output (id UINT64 NOT NULL) TYPE VOID SET ('worker:8080' AS "SINK"."HOST"))"})
    {
        const auto applied = submit(*coordinator, setup, Bridge::WaitMode::None);
        ASSERT_TRUE(Bridge::isOk(applied.error)) << std::string_view{applied.error.msg.data(), applied.error.msg.size()};
    }

    constexpr std::string_view query = "SELECT * FROM input INTO output";
    const auto outcome = coordinator->submit(rust::Str{query.data(), query.size()}, Bridge::WaitMode::UntilCompleted, 200, false);

    EXPECT_EQ(outcome.error.code, static_cast<uint16_t>(ErrorCode::QueryWaitTimeout))
        << std::string_view{outcome.error.msg.data(), outcome.error.msg.size()};
    EXPECT_NE(outcome.query_id, 0);
}

/// A stopped query has no failure, so the wait on it ends as cancelled.
/// The source never finishes, so only the stop ends the query.
TEST_F(SubmitWaitTest, AWaitOnAStoppedQueryIsCancelled)
{
    const auto coordinator
        = Bridge::start_coordinator(Bridge::StateBackendKind::Memory, rust::Str{}, Bridge::WorkerMode::Embedded, rust::Str{});
    for (const std::string_view setup :
         {"CREATE WORKER 'localhost:18280' SET ('localhost:19290' AS DATA)",
          "CREATE LOGICAL SOURCE input (id FLOAT64 NOT NULL)",
          R"(CREATE PHYSICAL SOURCE FOR input TYPE Generator SET (
              'ONE' AS "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
              1 AS "SOURCE".SEED,
              'NORMAL_DISTRIBUTION FLOAT64 0 10' AS "SOURCE".GENERATOR_SCHEMA,
              'CSV' AS INPUT_FORMATTER."TYPE",
              'localhost:18280' AS "SOURCE".HOST))",
          R"(CREATE SINK output (id FLOAT64 NOT NULL) TYPE VOID SET ('localhost:18280' AS "SINK"."HOST"))"})
    {
        const auto applied = submit(*coordinator, setup, Bridge::WaitMode::None);
        ASSERT_TRUE(Bridge::isOk(applied.error)) << setup << ": " << std::string_view{applied.error.msg.data(), applied.error.msg.size()};
    }

    constexpr std::chrono::milliseconds timeout{30000};
    constexpr std::chrono::milliseconds retryInterval{50};
    auto waited = std::async(
        std::launch::async,
        [&coordinator, timeout]
        {
            constexpr std::string_view query = "SELECT * FROM input INTO output";
            return coordinator->submit(
                rust::Str{query.data(), query.size()}, Bridge::WaitMode::UntilCompleted, static_cast<uint64_t>(timeout.count()), false);
        });

    /// The stop applies to nothing until the query exists, so it is retried.
    constexpr std::string_view stop = "STOP QUERY WHERE id = 1";
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        const auto stopped = coordinator->submit(rust::Str{stop.data(), stop.size()}, Bridge::WaitMode::None, 0, true);
        ASSERT_TRUE(Bridge::isOk(stopped.error)) << std::string_view{stopped.error.msg.data(), stopped.error.msg.size()};
        if (std::string{stopped.result}.contains(R"("id": 1)"))
        {
            break;
        }
        std::this_thread::sleep_for(retryInterval);
    }

    const auto outcome = waited.get();
    EXPECT_EQ(outcome.error.code, static_cast<uint16_t>(ErrorCode::QueryWaitCancelled))
        << std::string_view{outcome.error.msg.data(), outcome.error.msg.size()};
    EXPECT_EQ(outcome.query_id, 1);
}

}
