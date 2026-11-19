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
#include <string>
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

Bridge::StatementOutcome submit(const Bridge::Coordinator& coordinator, const std::string_view sql)
{
    return coordinator.submit(rust::Str{sql.data(), sql.size()}, Bridge::WaitMode::None, 0, false);
}

std::string expectApplied(const Bridge::Coordinator& coordinator, const std::string_view sql)
{
    const auto outcome = submit(coordinator, sql);
    EXPECT_TRUE(Bridge::isOk(outcome.error)) << sql << ": " << std::string_view{outcome.error.msg.data(), outcome.error.msg.size()};
    return std::string{outcome.result};
}

/// The sources and sinks of every kind, as the catalog lists them.
std::string connectors(const Bridge::Coordinator& coordinator)
{
    return expectApplied(coordinator, "SHOW PHYSICAL SOURCES") + expectApplied(coordinator, "SHOW SINKS");
}

}

class QueryPlanningTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("QueryPlanningTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(QueryPlanningTest, AnInvalidOptimizerOptionFailsTheStart)
{
    constexpr std::string_view optimizer = R"({"optimizer.no_such_option": "1"})";
    EXPECT_THROW(
        (void)Bridge::start_coordinator(
            Bridge::StateBackendKind::Memory, rust::Str{}, Bridge::WorkerMode::Remote, rust::Str{optimizer.data(), optimizer.size()}),
        rust::Error);
}

/// The source worker has no capacity, so the query needs a network channel between the two workers.
/// The optimizer creates a source and a sink for that channel, which only the created query keeps.
TEST_F(QueryPlanningTest, ExplainLeavesTheCatalogUnchangedAndTheQueryKeepsItsChannel)
{
    const auto coordinator
        = Bridge::start_coordinator(Bridge::StateBackendKind::Memory, rust::Str{}, Bridge::WorkerMode::Remote, rust::Str{});
    for (const std::string_view setup :
         {R"(CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA))",
          R"(CREATE WORKER 'source-node:8080' SET ('source-node:9090' AS DATA, 0 AS "MAX_OPERATORS", 'sink-node:8080' AS "DOWNSTREAM"))",
          "CREATE LOGICAL SOURCE input (a UINT64 NOT NULL)",
          R"(CREATE PHYSICAL SOURCE FOR input TYPE File
              SET ('source-node:8080' AS "SOURCE"."HOST", 'input.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE"))",
          R"(CREATE SINK output (a UINT64 NOT NULL) TYPE VOID SET ('sink-node:8080' AS "SINK"."HOST"))"})
    {
        expectApplied(*coordinator, setup);
    }
    const auto before = connectors(*coordinator);

    expectApplied(*coordinator, "EXPLAIN SELECT * FROM input WHERE a > UINT64(1) INTO output");
    EXPECT_EQ(connectors(*coordinator), before);

    expectApplied(*coordinator, "SELECT * FROM input WHERE a > UINT64(1) INTO output");
    const auto after = connectors(*coordinator);
    EXPECT_TRUE(after.contains("NETWORK")) << after;
}

}
