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

#include <cstdint>
#include <string_view>

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

}
