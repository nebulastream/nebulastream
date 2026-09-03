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

#include <QueryOptimizerConfiguration.hpp>
#include <SqlPlanner.hpp>

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <coordinator/lib.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <PlannerBridge.hpp>
#include <TransactionalCatalog.hpp>

namespace
{
/// Split a batch of setup DDL into individual statements. The binder plans one statement at a time, and none of the DDL
/// used here embeds a semicolon inside a literal, so a plain split on ';' is sufficient.
std::vector<std::string> splitStatements(const std::string_view sql)
{
    std::vector<std::string> statements;
    for (std::size_t start = 0; start < sql.size();)
    {
        const auto semicolon = sql.find(';', start);
        const auto piece = sql.substr(start, (semicolon == std::string_view::npos ? sql.size() : semicolon) - start);
        if (const auto first = piece.find_first_not_of(" \t\r\n"); first != std::string_view::npos)
        {
            statements.emplace_back(piece.substr(first, piece.find_last_not_of(" \t\r\n") - first + 1));
        }
        if (semicolon == std::string_view::npos)
        {
            break;
        }
        start = semicolon + 1;
    }
    return statements;
}

/// A throwaway catalog seeded from SQL DDL, paired with the query to place against it. Owns the `TestTransactionContext` so
/// the in-memory catalog outlives every optimizer run that reads it.
struct Prepared
{
    rust::Box<NES::TestTransactionContext> context;
    std::string query;

    /// Plan the query through the real `SqlPlanner`, which binds it and runs the optimizer, and return the per-worker
    /// placement it produces. A placement failure surfaces as the thrown `Exception` the negative tests expect.
    std::unordered_map<NES::Host, std::vector<NES::LogicalPlan>> optimize() const
    {
        auto output
            = NES::
                  SqlPlanner{std::make_shared<NES::TransactionalCatalog>(context->context(), NES::RequireHostConfig{}), NES::QueryOptimizerConfiguration{}, NES::RequireHostConfig{}}
                      .plan(query);
        if (!output)
        {
            throw std::move(output).error();
        }
        return std::move(output->fragments);
    }
};

/// Seed a fresh in-memory catalog with `setupSql` (a `;`-separated batch of `CREATE` statements) and pair it with
/// `query`. Each setup statement is planned by the real `plan_sql` and executed against the transaction the optimizer reads.
Prepared loadAndBind(const std::string_view setupSql, const std::string_view query)
{
    auto context = NES::create_test_planner_context();
    for (const auto& statement : splitStatements(setupSql))
    {
        const auto planned = NES::plan_sql(context->context(), rust::Str{statement.data(), statement.size()}, rust::Str{}, rust::Str{});
        context->execute_seed_statement(planned.json);
    }
    return Prepared{.context = std::move(context), .query = std::string{query}};
}
}

namespace NES
{
class DistributedPlanningTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("DistributedPlanning.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup DistributedPlanning class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }

    static void TearDownTestSuite() { NES_INFO("Tear down DistributedPlanning class."); }
};

///NOLINTBEGIN(bugprone-unchecked-optional-access, readability-identifier-length)
TEST_F(DistributedPlanningTest, BasicPlacementSingleNode)
{
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'localhost:8080' SET ('localhost:9090' AS DATA, 10 AS "CAPACITY");
        CREATE LOGICAL SOURCE mock_source (a UINT64 NOT NULL, b UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR mock_source TYPE File
            SET ('localhost:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK mock_sink (a UINT64 NOT NULL, b UINT64 NOT NULL) TYPE VOID SET ('localhost:8080' AS "SINK"."HOST");
        )",
        "SELECT * FROM mock_source WHERE a > b INTO mock_sink");
    auto plan = prepared.optimize();

    const LogicalPlan localPlan = plan[Host("localhost:8080")].front();
    const auto root = localPlan.getRootOperators();
    EXPECT_TRUE(root.size() == 1);
    const auto sink = root.back().tryGetAs<SinkLogicalOperator>();
    ASSERT_TRUE(sink.has_value());
    EXPECT_EQ(sink->get().getSinkDescriptor()->getSinkType(), "VOID");
    const auto leaf = getLeafOperators(localPlan);
    EXPECT_TRUE(leaf.size() == 1);
    const auto source = leaf.back().tryGetAs<SourceDescriptorLogicalOperator>();
    ASSERT_TRUE(source.has_value());
    EXPECT_EQ(source->get().getSourceDescriptor().getSourceType(), "FILE");
}

TEST_F(DistributedPlanningTest, BasicPlacementTwoNodes)
{
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA, 1 AS "CAPACITY");
        CREATE WORKER 'source-node:8080' SET ('source-node:9090' AS DATA, 0 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE LOGICAL SOURCE mock_source (a UINT64 NOT NULL, b UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR mock_source TYPE File
            SET ('source-node:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK mock_sink (a UINT64 NOT NULL, b UINT64 NOT NULL) TYPE VOID SET ('sink-node:8080' AS "SINK"."HOST");
        )",
        "SELECT * FROM mock_source WHERE a > b INTO mock_sink");
    auto plan = prepared.optimize();

    const auto sourceNodePlan = plan[Host("source-node:8080")].front();
    const auto root = sourceNodePlan.getRootOperators();
    EXPECT_TRUE(root.size() == 1);
    const auto networkSink = root.back().tryGetAs<SinkLogicalOperator>();
    EXPECT_TRUE(networkSink.has_value());
    EXPECT_EQ(networkSink->get().getSinkDescriptor()->getSinkType(), "NETWORK");
    const auto sources = getOperatorByType<SourceDescriptorLogicalOperator>(sourceNodePlan);
    EXPECT_TRUE(sources.size() == 1);
    EXPECT_EQ(sources.front().get().getSourceDescriptor().getSourceType(), "FILE");

    const auto sinkNodePlan = plan[Host("sink-node:8080")].front();
    const auto leaf = getLeafOperators(sinkNodePlan);
    EXPECT_TRUE(leaf.size() == 1);
    const auto networkSource = leaf.back().tryGetAs<SourceDescriptorLogicalOperator>();
    EXPECT_TRUE(networkSource.has_value());
    EXPECT_EQ(networkSource->get().getSourceDescriptor().getSourceType(), "NETWORK");
}

TEST_F(DistributedPlanningTest, JoinPlacementWithOneSelection)
{
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA, 10 AS "CAPACITY");
        CREATE WORKER 'source-node0:8080' SET ('source-node0:9090' AS DATA, 10 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'source-node1:8080' SET ('source-node1:9090' AS DATA, 10 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE LOGICAL SOURCE stream0 (ts0 UINT64 NOT NULL, id0 UINT64 NOT NULL);
        CREATE LOGICAL SOURCE stream1 (ts1 UINT64 NOT NULL, id1 UINT64 NOT NULL, a UINT64 NOT NULL, b UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR stream0 TYPE File
            SET ('source-node0:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream1 TYPE File
            SET ('source-node1:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK sink (start UINT64 NOT NULL, end UINT64 NOT NULL, ts0 UINT64 NOT NULL, id0 UINT64 NOT NULL, ts1 UINT64 NOT NULL, id1 UINT64 NOT NULL, a UINT64 NOT NULL, b UINT64 NOT NULL) TYPE VOID
            SET ('sink-node:8080' AS "SINK"."HOST");
        )",
        "SELECT * FROM (SELECT * FROM stream0) INNER JOIN (SELECT * FROM stream1 WHERE a < b) ON id0 = id1 WINDOW TUMBLING (ts0, ts1, "
        "size 1 sec) INTO sink");
    auto plan = prepared.optimize();

    const auto sourceNode0Plan = plan[Host("source-node0:8080")].front();
    EXPECT_EQ(flatten(sourceNode0Plan).size(), 3);
    EXPECT_EQ(getLeafOperators(sourceNode0Plan).size(), 1);
    EXPECT_EQ(
        getLeafOperators(sourceNode0Plan)
            .front()
            .getAs<SourceDescriptorLogicalOperator>()
            .get()
            .getSourceDescriptor()
            .getLogicalSource()
            .getLogicalSourceName(),
        Identifier::parse("STREAM0"));
    EXPECT_EQ(sourceNode0Plan.getRootOperators().size(), 1);
    EXPECT_EQ(sourceNode0Plan.getRootOperators().front().getAs<SinkLogicalOperator>().get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto sourceNode1Plan = plan[Host("source-node1:8080")].front();
    EXPECT_EQ(flatten(sourceNode1Plan).size(), 4);
    EXPECT_EQ(getLeafOperators(sourceNode1Plan).size(), 1);
    EXPECT_EQ(
        getLeafOperators(sourceNode1Plan)
            .front()
            .getAs<SourceDescriptorLogicalOperator>()
            .get()
            .getSourceDescriptor()
            .getLogicalSource()
            .getLogicalSourceName(),
        Identifier::parse("STREAM1"));
    EXPECT_EQ(sourceNode1Plan.getRootOperators().size(), 1);
    EXPECT_EQ(sourceNode1Plan.getRootOperators().front().getAs<SinkLogicalOperator>().get().getSinkDescriptor()->getSinkType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(sourceNode1Plan).size(), 1);

    const auto sinkNodePlan = plan[Host("sink-node:8080")].front();
    EXPECT_EQ(flatten(sinkNodePlan).size(), 4);
    EXPECT_EQ(getLeafOperators(sinkNodePlan).size(), 2);
    EXPECT_EQ(
        getLeafOperators(sinkNodePlan)[0].getAs<SourceDescriptorLogicalOperator>().get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(
        getLeafOperators(sinkNodePlan)[1].getAs<SourceDescriptorLogicalOperator>().get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(sinkNodePlan.getRootOperators().size(), 1);
    EXPECT_EQ(sinkNodePlan.getRootOperators().front().getAs<SinkLogicalOperator>().get().getSinkName(), Identifier::parse("SINK"));
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkNodePlan).size(), 1);
}

TEST_F(DistributedPlanningTest, PlacementWithThreeNodes)
{
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA, 10 AS "CAPACITY");
        CREATE WORKER 'source-node0:8080' SET ('source-node0:9090' AS DATA, 4 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'source-node1:8080' SET ('source-node1:9090' AS DATA, 3 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE LOGICAL SOURCE stream0 (ts0 UINT64 NOT NULL, id0 UINT64 NOT NULL, a UINT64 NOT NULL, b UINT64 NOT NULL);
        CREATE LOGICAL SOURCE stream1 (ts1 UINT64 NOT NULL, id1 UINT64 NOT NULL, c UINT64 NOT NULL, d UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR stream0 TYPE File
            SET ('source-node0:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream1 TYPE File
            SET ('source-node1:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK sink (start UINT64 NOT NULL, end UINT64 NOT NULL, ts0 UINT64 NOT NULL, a UINT64 NOT NULL, id0 UINT64 NOT NULL, ts1 UINT64 NOT NULL, id1 UINT64 NOT NULL, c UINT64 NOT NULL, d UINT64 NOT NULL) TYPE VOID
            SET ('sink-node:8080' AS "SINK"."HOST");
        )",
        "SELECT * FROM (SELECT ts0, a, id0 FROM stream0 WHERE a != b) INNER JOIN (SELECT * FROM stream1 WHERE c < d) ON id0 = id1 WINDOW "
        "TUMBLING (ts0, ts1, size 1 sec) INTO sink");
    auto plan = prepared.optimize();

    const auto plan0 = plan[Host("sink-node:8080")].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[0].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[1].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(flatten(plan0).size(), 4);

    const auto plan1 = plan[Host("source-node0:8080")].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).front().get().getSinkDescriptor()->getSinkType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<ProjectionLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(flatten(plan1).size(), 5);

    const auto plan2 = plan[Host("source-node1:8080")].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).front().get().getSinkDescriptor()->getSinkType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(flatten(plan2).size(), 4);
}

TEST_F(DistributedPlanningTest, JoinPlacementWithLimitedCapacity)
{
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA, 10 AS "CAPACITY");
        CREATE WORKER 'source-node0:8080' SET ('source-node0:9090' AS DATA, 10 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'source-node1:8080' SET ('source-node1:9090' AS DATA, 1 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE LOGICAL SOURCE stream0 (ts0 UINT64 NOT NULL, id0 UINT64 NOT NULL);
        CREATE LOGICAL SOURCE stream1 (ts1 UINT64 NOT NULL, id1 UINT64 NOT NULL, a UINT64 NOT NULL, b UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR stream0 TYPE File
            SET ('source-node0:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream1 TYPE File
            SET ('source-node1:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK sink (start UINT64 NOT NULL, end UINT64 NOT NULL, ts0 UINT64 NOT NULL, id0 UINT64 NOT NULL, ts1 UINT64 NOT NULL, id1 UINT64 NOT NULL, a UINT64 NOT NULL, b UINT64 NOT NULL) TYPE VOID
            SET ('sink-node:8080' AS "SINK"."HOST");
        )",
        "SELECT * FROM (SELECT * FROM stream0) INNER JOIN (SELECT * FROM stream1 WHERE a < b) ON id0 = id1 WINDOW TUMBLING (ts0, ts1, "
        "size 1 sec) INTO sink");
    auto plan = prepared.optimize();

    const auto plan0 = plan[Host("sink-node:8080")].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[0].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[1].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(flatten(plan0).size(), 5);

    const auto plan1 = plan[Host("source-node0:8080")].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).front().get().getSinkDescriptor()->getSinkType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(flatten(plan1).size(), 3);

    const auto plan2 = plan[Host("source-node1:8080")].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).front().get().getSinkDescriptor()->getSinkType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(flatten(plan2).size(), 3);
}

TEST_F(DistributedPlanningTest, JoinPlacementWithLimitedCapacityOnTwoNodes)
{
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA, 10 AS "CAPACITY");
        CREATE WORKER 'source-node:8080' SET ('source-node:9090' AS DATA, 3 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE LOGICAL SOURCE stream0 (ts0 UINT64 NOT NULL, id0 UINT64 NOT NULL);
        CREATE LOGICAL SOURCE stream1 (ts1 UINT64 NOT NULL, id1 UINT64 NOT NULL, a UINT64 NOT NULL, b UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR stream0 TYPE File
            SET ('source-node:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream1 TYPE File
            SET ('source-node:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK sink (start UINT64 NOT NULL, end UINT64 NOT NULL, ts0 UINT64 NOT NULL, id0 UINT64 NOT NULL, ts1 UINT64 NOT NULL, id1 UINT64 NOT NULL, a UINT64 NOT NULL, b UINT64 NOT NULL) TYPE VOID
            SET ('sink-node:8080' AS "SINK"."HOST");
        )",
        "SELECT * FROM (SELECT * FROM stream0) INNER JOIN (SELECT * FROM stream1 WHERE a < b) ON id0 = id1 WINDOW TUMBLING (ts0, ts1, "
        "size 1 sec) INTO sink");
    auto plan = prepared.optimize();

    const auto sinkPlan = plan[Host("sink-node:8080")].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).front().get().getSinkDescriptor()->getSinkType(), "VOID");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(flatten(sinkPlan).size(), 4);

    const auto sourcePlan1 = plan[Host("source-node:8080")][0];
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan1)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan1)[0].get().getSourceDescriptor().getSourceType(), "FILE");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(flatten(sourcePlan1).size(), 3);

    const auto sourcePlan2 = plan[Host("source-node:8080")][1];
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan2)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan2)[0].get().getSourceDescriptor().getSourceType(), "FILE");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(flatten(sourcePlan2).size(), 4);
}

TEST_F(DistributedPlanningTest, FourWayJoin)
{
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'host1:8080' SET ('host1:9090' AS DATA, 5 AS "CAPACITY", 'host2:8080' AS "DOWNSTREAM");
        CREATE WORKER 'host2:8080' SET ('host2:9090' AS DATA, 255 AS "CAPACITY");
        CREATE LOGICAL SOURCE stream (id UINT64 NOT NULL, value UINT64 NOT NULL, ts UINT64 NOT NULL);
        CREATE LOGICAL SOURCE stream2 (id2 UINT64 NOT NULL, value2 UINT64 NOT NULL, ts2 UINT64 NOT NULL);
        CREATE LOGICAL SOURCE stream4 (id4 UINT64 NOT NULL, value4 UINT64 NOT NULL, ts4 UINT64 NOT NULL);
        CREATE LOGICAL SOURCE stream4_1 (id4_1 UINT64 NOT NULL, value4_1 UINT64 NOT NULL, ts4_1 UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR stream TYPE File
            SET ('host1:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream2 TYPE File
            SET ('host1:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream4 TYPE File
            SET ('host1:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream4_1 TYPE File
            SET ('host1:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK sinkStreamStream2Stream4Stream4_1
            (start UINT64 NOT NULL, end UINT64 NOT NULL, start2 UINT64 NOT NULL, end2 UINT64 NOT NULL, start1 UINT64 NOT NULL, end1 UINT64 NOT NULL, id UINT64 NOT NULL, value UINT64 NOT NULL, ts UINT64 NOT NULL,
             id2 UINT64 NOT NULL, value2 UINT64 NOT NULL, ts2 UINT64 NOT NULL, id4 UINT64 NOT NULL, value4 UINT64 NOT NULL, ts4 UINT64 NOT NULL, id4_1 UINT64 NOT NULL, value4_1 UINT64 NOT NULL, ts4_1 UINT64 NOT NULL)
            TYPE VOID SET ('host2:8080' AS "SINK"."HOST");
        )",
        R"(SELECT * FROM (
            SELECT * FROM (
              SELECT start as start2, end as end2, start1, end1, id, value, ts, id2, value2, ts2, id4, value4, ts4
              FROM (
                SELECT * FROM (
                  SELECT start as start1, end as end1, id, value, ts, id2, value2, ts2
                  FROM (
                    SELECT * FROM (SELECT * FROM stream)
                    INNER JOIN (SELECT * FROM stream2) ON id = id2 WINDOW TUMBLING (ts, ts2, size 1 sec)
                  )
                )
                INNER JOIN (SELECT * FROM stream4) ON id = id4 WINDOW TUMBLING (ts, ts4, size 1 sec)
              )
            )
            INNER JOIN (SELECT * FROM stream4_1) ON id = id4_1 WINDOW TUMBLING (ts, ts4_1, size 1 sec)
          )
          INTO sinkStreamStream2Stream4Stream4_1)");
    auto plan = prepared.optimize();

    for (const auto& [node, plans] : plan)
    {
        for (const auto& localPlan : plans)
        {
            NES_DEBUG("Plan on node {}: \n{}", node, localPlan);
        }
    }
}

TEST_F(DistributedPlanningTest, BridgePlacement)
{
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'source-node:8080' SET ('source-node:9090' AS DATA, 10 AS "CAPACITY", 'intermediate-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'intermediate-node:8080' SET ('intermediate-node:9090' AS DATA, 0 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA, 10 AS "CAPACITY");
        CREATE LOGICAL SOURCE stream (ts UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR stream TYPE File
            SET ('source-node:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK sink (ts UINT64 NOT NULL) TYPE VOID SET ('sink-node:8080' AS "SINK"."HOST");
        )",
        "SELECT * FROM stream INTO sink");
    auto plan = prepared.optimize();

    const auto sourcePlan = plan[Host("source-node:8080")].front();
    EXPECT_EQ(flatten(sourcePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan)[0].get().getSourceDescriptor().getSourceType(), "FILE");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto intermediatePlan = plan[Host("intermediate-node:8080")].front();
    EXPECT_EQ(flatten(intermediatePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan)[0].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto sinkPlan = plan[Host("sink-node:8080")].front();
    EXPECT_EQ(flatten(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].get().getSinkDescriptor()->getSinkType(), "VOID");
}

TEST_F(DistributedPlanningTest, LongBridgePlacement)
{
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'source-node:8080' SET ('source-node:9090' AS DATA, 0 AS "CAPACITY", 'intermediate-node0:8080' AS "DOWNSTREAM");
        CREATE WORKER 'intermediate-node0:8080'
            SET ('intermediate-node0:9090' AS DATA, 0 AS "CAPACITY", 'intermediate-node1:8080' AS "DOWNSTREAM");
        CREATE WORKER 'intermediate-node1:8080'
            SET ('intermediate-node1:9090' AS DATA, 0 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA, 0 AS "CAPACITY");
        CREATE LOGICAL SOURCE stream (ts UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR stream TYPE File
            SET ('source-node:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK sink (ts UINT64 NOT NULL) TYPE VOID SET ('sink-node:8080' AS "SINK"."HOST");
        )",
        "SELECT * FROM stream INTO sink");
    auto plan = prepared.optimize();

    const auto sourcePlan = plan[Host("source-node:8080")].front();
    EXPECT_EQ(flatten(sourcePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan)[0].get().getSourceDescriptor().getSourceType(), "FILE");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto intermediatePlan0 = plan[Host("intermediate-node0:8080")].front();
    EXPECT_EQ(flatten(intermediatePlan0).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan0).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan0)[0].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan0).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan0)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto intermediatePlan1 = plan[Host("intermediate-node1:8080")].front();
    EXPECT_EQ(flatten(intermediatePlan1).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan1).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan1)[0].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan1)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto sinkPlan = plan[Host("sink-node:8080")].front();
    EXPECT_EQ(flatten(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].get().getSinkDescriptor()->getSinkType(), "VOID");
}

TEST_F(DistributedPlanningTest, BridgePlacementJoin)
{
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'source-node:8080' SET ('source-node:9090' AS DATA, 3 AS "CAPACITY", 'intermediate-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'intermediate-node:8080' SET ('intermediate-node:9090' AS DATA, 0 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA, 10 AS "CAPACITY");
        CREATE LOGICAL SOURCE stream0 (ts0 UINT64 NOT NULL, id0 UINT64 NOT NULL);
        CREATE LOGICAL SOURCE stream1 (ts1 UINT64 NOT NULL, id1 UINT64 NOT NULL, a UINT64 NOT NULL, b UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR stream0 TYPE File
            SET ('source-node:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream1 TYPE File
            SET ('source-node:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK sink (start UINT64 NOT NULL, end UINT64 NOT NULL, ts0 UINT64 NOT NULL, id0 UINT64 NOT NULL, ts1 UINT64 NOT NULL, id1 UINT64 NOT NULL, a UINT64 NOT NULL, b UINT64 NOT NULL) TYPE VOID
            SET ('sink-node:8080' AS "SINK"."HOST");
        )",
        "SELECT * FROM (SELECT * FROM stream0) INNER JOIN (SELECT * FROM stream1 WHERE a < b) ON id0 = id1 WINDOW TUMBLING (ts0, ts1, "
        "size 1 sec) INTO sink");
    auto plan = prepared.optimize();

    const auto sourcePlan1 = plan[Host("source-node:8080")][0];
    EXPECT_EQ(flatten(sourcePlan1).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan1)[0].get().getSourceDescriptor().getSourceType(), "FILE");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan1)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto sourcePlan2 = plan[Host("source-node:8080")][1];
    EXPECT_EQ(flatten(sourcePlan2).size(), 4);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan2)[0].get().getSourceDescriptor().getSourceType(), "FILE");
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan2)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto intermediatePlan1 = plan[Host("intermediate-node:8080")][0];
    EXPECT_EQ(flatten(intermediatePlan1).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan1).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan1)[0].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan1)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto intermediatePlan2 = plan[Host("intermediate-node:8080")][1];
    EXPECT_EQ(flatten(intermediatePlan2).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan2).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan2)[0].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan2)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto sinkPlan = plan[Host("sink-node:8080")].front();
    EXPECT_EQ(flatten(sinkPlan).size(), 4);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[1].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].get().getSinkDescriptor()->getSinkType(), "VOID");
}

TEST_F(DistributedPlanningTest, ComplexJoinQuery)
{
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'source-node0:8080' SET ('source-node0:9090' AS DATA, 3 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'source-node1:8080' SET ('source-node1:9090' AS DATA, 1 AS "CAPACITY", 'intermediate-node0:8080' AS "DOWNSTREAM");
        CREATE WORKER 'source-node2:8080' SET ('source-node2:9090' AS DATA, 0 AS "CAPACITY", 'intermediate-node1:8080' AS "DOWNSTREAM");
        CREATE WORKER 'intermediate-node0:8080' SET ('intermediate-node0:9090' AS DATA, 0 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'intermediate-node1:8080' SET ('intermediate-node1:9090' AS DATA, 1 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA, 6 AS "CAPACITY");
        CREATE LOGICAL SOURCE stream0 (ts0 UINT64 NOT NULL, id0 UINT64 NOT NULL);
        CREATE LOGICAL SOURCE stream1 (ts1 UINT64 NOT NULL, id1 UINT64 NOT NULL);
        CREATE LOGICAL SOURCE stream2 (ts2 UINT64 NOT NULL, id2 UINT64 NOT NULL);
        CREATE LOGICAL SOURCE stream3 (ts3 UINT64 NOT NULL, id3 UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR stream0 TYPE File
            SET ('source-node0:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream1 TYPE File
            SET ('source-node0:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream2 TYPE File
            SET ('source-node1:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream3 TYPE File
            SET ('source-node2:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK sink
            (start UINT64 NOT NULL, end UINT64 NOT NULL, start2 UINT64 NOT NULL, end2 UINT64 NOT NULL, start1 UINT64 NOT NULL, end1 UINT64 NOT NULL, ts0 UINT64 NOT NULL, id0 UINT64 NOT NULL, ts1 UINT64 NOT NULL,
             id1 UINT64 NOT NULL, ts2 UINT64 NOT NULL, id2 UINT64 NOT NULL, ts3 UINT64 NOT NULL, id3 UINT64 NOT NULL)
            TYPE VOID SET ('sink-node:8080' AS "SINK"."HOST");
        )",
        R"(SELECT * FROM (
            SELECT * FROM (
              SELECT start as start2, end as end2, start1, end1, ts0, id0, ts1, id1, ts2, id2
              FROM (
                SELECT * FROM (
                  SELECT start as start1, end as end1, ts0, id0, ts1, id1
                  FROM (
                    SELECT * FROM (SELECT * FROM stream0)
                    INNER JOIN (SELECT * FROM stream1) ON id0 = id1 WINDOW TUMBLING (ts0, ts1, size 1 sec)
                  )
                )
                INNER JOIN (SELECT * FROM stream2) ON id1 = id2 WINDOW TUMBLING (ts0, ts2, size 1 sec)
              )
            )
            INNER JOIN (SELECT * FROM stream3) ON id2 = id3 WINDOW TUMBLING (ts0, ts3, size 1 sec)
          )
          INTO sink)");
    auto plan = prepared.optimize();

    for (const auto& [node, localPlans] : plan)
    {
        for (const auto& localPlan : localPlans)
        {
            NES_DEBUG("Plan on node {}: \n{}", node, localPlan);
        }
    }
    const auto sourceNode0Plan = plan[Host("source-node0:8080")].front();
    EXPECT_EQ(flatten(sourceNode0Plan).size(), 6);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode0Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode0Plan)[0].get().getSourceDescriptor().getSourceType(), "FILE");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode0Plan)[1].get().getSourceDescriptor().getSourceType(), "FILE");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sourceNode0Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourceNode0Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode0Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode0Plan)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto sourceNode1Plan = plan[Host("source-node1:8080")].front();
    EXPECT_EQ(flatten(sourceNode1Plan).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan)[0].get().getSourceDescriptor().getSourceType(), "FILE");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto sourceNode2Plan = plan[Host("source-node2:8080")].front();
    EXPECT_EQ(flatten(sourceNode2Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan)[0].get().getSourceDescriptor().getSourceType(), "FILE");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto intermediateNode0Plan = plan[Host("intermediate-node0:8080")].front();
    EXPECT_EQ(flatten(intermediateNode0Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode0Plan).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode0Plan)[0].get().getSourceDescriptor().getSourceType(),
        "NETWORK");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode0Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode0Plan)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto intermediateNode1Plan = plan[Host("intermediate-node1:8080")].front();
    EXPECT_EQ(flatten(intermediateNode1Plan).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode1Plan).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode1Plan)[0].get().getSourceDescriptor().getSourceType(),
        "NETWORK");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(intermediateNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode1Plan)[0].get().getSinkDescriptor()->getSinkType(), "NETWORK");

    const auto sinkPlan = plan[Host("sink-node:8080")].front();
    EXPECT_EQ(flatten(sinkPlan).size(), 8);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[1].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[2].get().getSourceDescriptor().getSourceType(), "NETWORK");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<ProjectionLogicalOperator>(sinkPlan).size(), 2);
    /// Watermark assignments are pushed fully upstream — stream2's watermark applies on source-node1,
    /// stream3's on intermediate-node1 — so the sink-node plan no longer carries any of its own.
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sinkPlan).size(), 0);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].get().getSinkDescriptor()->getSinkType(), "VOID");
}

TEST_F(DistributedPlanningTest, Disconnected)
{
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'source-node:8080' SET ('source-node:9090' AS DATA, 10 AS "CAPACITY", 'intermediate-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'intermediate-node:8080' SET ('intermediate-node:9090' AS DATA, 0 AS "CAPACITY");
        CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA, 10 AS "CAPACITY");
        CREATE LOGICAL SOURCE stream (ts UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR stream TYPE File
            SET ('source-node:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK sink (ts UINT64 NOT NULL) TYPE VOID SET ('sink-node:8080' AS "SINK"."HOST");
        )",
        "SELECT * FROM stream INTO sink");
    try
    {
        auto _ = prepared.optimize();
        FAIL() << "Expected Exception";
    }
    catch (const Exception& e)
    {
        EXPECT_EQ(e.code(), ErrorCode::PlacementFailure);
        EXPECT_TRUE(std::string_view(e.what()).find("topology is not connected") != std::string_view::npos)
            << "Expected 'topology is not connected' in: " << e.what();
        EXPECT_TRUE(std::string_view(e.what()).find("No path from source worker") != std::string_view::npos)
            << "Expected 'No path from source worker' in: " << e.what();
    }
}

TEST_F(DistributedPlanningTest, NotEnoughCapacities)
{
    /// The projection a * 2 - > a is required so to ensure the query optimization does not push down the outer predicate a > b down and combine it with  b > a.
    /// If that would be the case, the assumptions about the number of operators would no longer hold.
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA, 1 AS "CAPACITY");
        CREATE WORKER 'source-node:8080' SET ('source-node:9090' AS DATA, 0 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE LOGICAL SOURCE mock_source (a UINT64 NOT NULL, b UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR mock_source TYPE File
            SET ('source-node:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK mock_sink (a UINT64 NOT NULL, b UINT64 NOT NULL) TYPE VOID SET ('sink-node:8080' AS "SINK"."HOST");
        )",
        "SELECT * FROM (SELECT a * UINT64(2) AS a, b FROM mock_source WHERE a > b) WHERE b > a INTO mock_sink");
    try
    {
        auto _ = prepared.optimize();
        FAIL() << "Expected Exception";
    }
    catch (const Exception& e)
    {
        EXPECT_EQ(e.code(), ErrorCode::PlacementFailure);
        EXPECT_TRUE(std::string_view(e.what()).find("capacity constraints") != std::string_view::npos)
            << "Expected 'capacity constraints' in: " << e.what();
    }
}

TEST_F(DistributedPlanningTest, MultiplePhysicalSources)
{
    /// Plan has a logical source that is referenced by 9 physical sources. 4 physical source on source and 5 on source2.
    /// Capacity prevents the union from beeing placed at the intermediate node
    const auto prepared = loadAndBind(
        R"(
        CREATE WORKER 'source-node2:8080' SET ('source-node2:9090' AS DATA, 10 AS "CAPACITY", 'intermediate-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'source-node:8080' SET ('source-node:9090' AS DATA, 10 AS "CAPACITY", 'intermediate-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'intermediate-node:8080' SET ('intermediate-node:9090' AS DATA, 0 AS "CAPACITY", 'sink-node:8080' AS "DOWNSTREAM");
        CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA, 10 AS "CAPACITY");
        CREATE LOGICAL SOURCE stream (ts UINT64 NOT NULL);
        CREATE PHYSICAL SOURCE FOR stream TYPE File SET ('source-node:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream TYPE File SET ('source-node:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream TYPE File SET ('source-node:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream TYPE File SET ('source-node:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream TYPE File SET ('source-node2:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream TYPE File SET ('source-node2:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream TYPE File SET ('source-node2:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream TYPE File SET ('source-node2:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE PHYSICAL SOURCE FOR stream TYPE File SET ('source-node2:8080' AS "SOURCE"."HOST", 'does_not_exist' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
        CREATE SINK sink (ts UINT64 NOT NULL) TYPE VOID SET ('sink-node:8080' AS "SINK"."HOST");
        )",
        "SELECT * FROM stream INTO sink");
    auto plan = prepared.optimize();

    const auto sinkPlans = plan[Host("sink-node:8080")];
    ASSERT_EQ(sinkPlans.size(), 1);
    EXPECT_EQ(flatten(sinkPlans.front()).size(), 9 + 1 + 1);

    const auto intermediatePlans = plan[Host("intermediate-node:8080")];
    ASSERT_EQ(intermediatePlans.size(), 9);
    EXPECT_EQ(flatten(intermediatePlans.front()).size(), 2);

    const auto source2Plans = plan[Host("source-node2:8080")];
    ASSERT_EQ(source2Plans.size(), 5);
    EXPECT_EQ(flatten(source2Plans.front()).size(), 2);

    const auto sourcePlans = plan[Host("source-node:8080")];
    ASSERT_EQ(sourcePlans.size(), 4);
    EXPECT_EQ(flatten(source2Plans.front()).size(), 2);
}

///NOLINTEND(bugprone-unchecked-optional-access, readability-identifier-length)
}
