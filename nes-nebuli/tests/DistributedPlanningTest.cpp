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

#include <filesystem>
#include <optional>

#include <gtest/gtest.h>

#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Statements/StatementHandler.hpp>
#include <yaml-cpp/node/parse.h>
#include <yaml-cpp/yaml.h>

#include <BaseUnitTest.hpp>
#include <QueryPlanning.hpp>

namespace NES::Test
{

struct Sink
{
    std::string name;
    std::vector<std::string> schema;
    std::string host;
};

struct LogicalSource
{
    std::string name;
    std::vector<std::string> schema;
};

struct PhysicalSource
{
    std::string logical;
    std::string host;
};

struct WorkerConfig
{
    std::string host;
    std::string grpc;
    size_t capacity;
    std::vector<std::string> downstream;
};

struct QueryConfig
{
    std::string query;
    std::vector<Sink> sinks;
    std::vector<LogicalSource> logical;
    std::vector<PhysicalSource> physical;
    std::vector<WorkerConfig> workers;
};
}

namespace YAML
{

template <>
struct convert<NES::Test::Sink>
{
    static bool decode(const Node& node, NES::Test::Sink& rhs)
    {
        rhs.name = node["name"].as<std::string>();
        rhs.schema = node["schema"].as<std::vector<std::string>>();
        rhs.host = node["host"].as<std::string>();
        return true;
    }
};

template <>
struct convert<NES::Test::LogicalSource>
{
    static bool decode(const Node& node, NES::Test::LogicalSource& rhs)
    {
        rhs.name = node["name"].as<std::string>();
        rhs.schema = node["schema"].as<std::vector<std::string>>();
        return true;
    }
};

template <>
struct convert<NES::Test::PhysicalSource>
{
    static bool decode(const Node& node, NES::Test::PhysicalSource& rhs)
    {
        rhs.logical = node["logical"].as<std::string>();
        rhs.host = node["host"].as<std::string>();
        return true;
    }
};

template <>
struct convert<NES::Test::WorkerConfig>
{
    static bool decode(const Node& node, NES::Test::WorkerConfig& rhs)
    {
        rhs.capacity = node["capacity"].as<size_t>();
        if (node["downstream"].IsDefined())
        {
            rhs.downstream = node["downstream"].as<std::vector<std::string>>();
        }
        rhs.grpc = node["grpc"].as<std::string>();
        rhs.host = node["host"].as<std::string>();

        return true;
    }
};

template <>
struct convert<NES::Test::QueryConfig>
{
    static bool decode(const Node& node, NES::Test::QueryConfig& rhs)
    {
        rhs.sinks = node["sinks"].as<std::vector<NES::Test::Sink>>();
        rhs.logical = node["logical"].as<std::vector<NES::Test::LogicalSource>>();
        rhs.physical = node["physical"].as<std::vector<NES::Test::PhysicalSource>>();
        rhs.workers = node["workers"].as<std::vector<NES::Test::WorkerConfig>>();
        rhs.query = node["query"].as<std::string>();
        return true;
    }
};
}

std::vector<NES::Statement> loadStatements(const NES::Test::QueryConfig& topologyConfig)
{
    const auto& [query, sinks, logical, physical, workers] = topologyConfig;
    std::vector<NES::Statement> statements;
    for (const auto& [host, grpc, capacity, downstream] : workers)
    {
        statements.emplace_back(NES::CreateWorkerStatement{.host = host, .grpc = grpc, .capacity = capacity, .downstream = downstream});
    }
    for (const auto& [name, schemaFields] : logical)
    {
        NES::Schema schema;
        for (const auto& schemaField : schemaFields)
        {
            schema.addField(schemaField, NES::DataType::Type::UINT64);
        }

        statements.emplace_back(NES::CreateLogicalSourceStatement{.name = name, .schema = schema});
    }

    for (const auto& [logical, host] : physical)
    {
        statements.emplace_back(NES::CreatePhysicalSourceStatement{
            .attachedTo = NES::LogicalSourceName(logical),
            .sourceType = "File",
            .workerId = host,
            .sourceConfig = {{"file_path", "does_not_exist"}},
            .parserConfig = {{"type", "CSV"}}});
    }
    for (const auto& [name, schemaFields, host] : sinks)
    {
        NES::Schema schema;
        for (const auto& schemaField : schemaFields)
        {
            schema.addField(schemaField, NES::DataType::Type::UINT64);
        }

        statements.emplace_back(
            NES::CreateSinkStatement{.name = name, .sinkType = "Void", .workerId = host, .schema = schema, .sinkConfig = {}});
    }
    statements.emplace_back(NES::ExplainQueryStatement{.plan = NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query)});
    return statements;
}

namespace NES
{
class QueryManagerTest : public Testing::BaseUnitTest
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

std::pair<QueryPlanningContext, PlanStage::BoundLogicalPlan> loadAndBind(std::string_view testFileName)
{
    auto sources = std::make_shared<SourceCatalog>();
    auto sinks = std::make_shared<SinkCatalog>();
    auto workers = std::make_shared<WorkerCatalog>();

    auto queryConfig = YAML::LoadFile(std::filesystem::path{NEBULI_TEST_DATA_DIR} / testFileName).as<Test::QueryConfig>();
    auto statements = loadStatements(queryConfig);

    TopologyStatementHandler topologyHandler{nullptr, workers};
    SinkStatementHandler sinkStatementHandler{sinks};
    SourceStatementHandler sourceStatementHandler{sources};

    handleStatements(statements, topologyHandler, sinkStatementHandler, sourceStatementHandler);
    return {
        QueryPlanningContext{
            .id = INVALID<LocalQueryId>,
            .sqlString = std::get<ExplainQueryStatement>(statements.back()).plan.getOriginalSql(),
            .sourceCatalog = sources,
            .sinkCatalog = sinks,
            .workerCatalog = workers},
        PlanStage::BoundLogicalPlan{std::get<ExplainQueryStatement>(statements.back()).plan}};
}

TEST_F(QueryManagerTest, BasicPlacementSingleNode)
{
    auto [ctx, boundPlan] = loadAndBind("basic_single_node.yaml");
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const LogicalPlan localPlan = plan[GrpcAddr("localhost:8080")].front();
    const auto root = localPlan.getRootOperators();
    EXPECT_TRUE(root.size() == 1);
    const auto sink = root.back().tryGetAs<SinkLogicalOperator>();
    ASSERT_TRUE(sink.has_value());
    EXPECT_EQ(sink->get().getSinkDescriptor()->getSinkType(), "Void");
    const auto leaf = getLeafOperators(localPlan);
    EXPECT_TRUE(leaf.size() == 1);
    const auto source = leaf.back().tryGetAs<SourceDescriptorLogicalOperator>();
    ASSERT_TRUE(source.has_value());
    EXPECT_EQ(source->get().getSourceDescriptor().getSourceType(), "File");
}

TEST_F(QueryManagerTest, BasicPlacementTwoNodes)
{
    auto [ctx, boundPlan] = loadAndBind("basic_two_nodes.yaml");
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto sourceNodePlan = plan[GrpcAddr("source-node:8080")].front();
    const auto root = sourceNodePlan.getRootOperators();
    EXPECT_TRUE(root.size() == 1);
    const auto networkSink = root.back().tryGetAs<SinkLogicalOperator>();
    EXPECT_TRUE(networkSink.has_value());
    EXPECT_EQ(networkSink->get().getSinkDescriptor()->getSinkType(), "Network");
    const auto sources = getOperatorByType<SourceDescriptorLogicalOperator>(sourceNodePlan);
    EXPECT_TRUE(sources.size() == 1);
    EXPECT_EQ(sources.front().get().getSourceDescriptor().getSourceType(), "File");

    const auto sinkNodePlan = plan[GrpcAddr("sink-node:8080")].front();
    const auto leaf = getLeafOperators(sinkNodePlan);
    EXPECT_TRUE(leaf.size() == 1);
    const auto networkSource = leaf.back().tryGetAs<SourceDescriptorLogicalOperator>();
    EXPECT_TRUE(networkSource.has_value());
    EXPECT_EQ(networkSource->get().getSourceDescriptor().getSourceType(), "Network");
}

TEST_F(QueryManagerTest, JoinPlacementWithOneSelection)
{
    auto [ctx, boundPlan] = loadAndBind("join_with_one_selection.yaml");
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto sourceNode0Plan = plan[GrpcAddr("source-node0:8080")].front();
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
        "stream0");
    EXPECT_EQ(sourceNode0Plan.getRootOperators().size(), 1);
    EXPECT_EQ(sourceNode0Plan.getRootOperators().front().getAs<SinkLogicalOperator>().get().getSinkDescriptor()->getSinkType(), "Network");

    const auto sourceNode1Plan = plan[GrpcAddr("source-node1:8080")].front();
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
        "stream1");
    EXPECT_EQ(sourceNode1Plan.getRootOperators().size(), 1);
    EXPECT_EQ(sourceNode1Plan.getRootOperators().front().getAs<SinkLogicalOperator>().get().getSinkDescriptor()->getSinkType(), "Network");
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(sourceNode1Plan).size(), 1);

    const auto sinkNodePlan = plan[GrpcAddr("sink-node:8080")].front();
    EXPECT_EQ(flatten(sinkNodePlan).size(), 4);
    EXPECT_EQ(getLeafOperators(sinkNodePlan).size(), 2);
    EXPECT_EQ(
        getLeafOperators(sinkNodePlan)[0].getAs<SourceDescriptorLogicalOperator>().get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(
        getLeafOperators(sinkNodePlan)[1].getAs<SourceDescriptorLogicalOperator>().get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(sinkNodePlan.getRootOperators().size(), 1);
    EXPECT_EQ(sinkNodePlan.getRootOperators().front().getAs<SinkLogicalOperator>().get().getSinkName(), "sink");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkNodePlan).size(), 1);
}

TEST_F(QueryManagerTest, PlacementWithThreeNodes)
{
    auto [ctx, boundPlan] = loadAndBind("three_nodes.yaml");
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto plan0 = plan[GrpcAddr("sink-node:8080")].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[0].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[1].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(flatten(plan0).size(), 4);

    const auto plan1 = plan[GrpcAddr("source-node0:8080")].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).front().get().getSinkDescriptor()->getSinkType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<ProjectionLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(flatten(plan1).size(), 5);

    const auto plan2 = plan[GrpcAddr("source-node1:8080")].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).front().get().getSinkDescriptor()->getSinkType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(flatten(plan2).size(), 4);
}

TEST_F(QueryManagerTest, JoinPlacementWithLimitedCapacity)
{
    auto [ctx, boundPlan] = loadAndBind("join_with_limited_capacity.yaml");
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto plan0 = plan[GrpcAddr("sink-node:8080")].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[0].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[1].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(flatten(plan0).size(), 5);

    const auto plan1 = plan[GrpcAddr("source-node0:8080")].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).front().get().getSinkDescriptor()->getSinkType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(flatten(plan1).size(), 3);

    const auto plan2 = plan[GrpcAddr("source-node1:8080")].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).front().get().getSinkDescriptor()->getSinkType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(flatten(plan2).size(), 3);
}

TEST_F(QueryManagerTest, JoinPlacementWithLimitedCapacityOnTwoNodes)
{
    auto [ctx, boundPlan] = loadAndBind("join_with_limited_capacity_on_two_nodes.yaml");
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto sinkPlan = plan[GrpcAddr("sink-node:8080")].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).front().get().getSinkDescriptor()->getSinkType(), "Void");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(flatten(sinkPlan).size(), 4);

    const auto sourcePlan1 = plan[GrpcAddr("source-node:8080")][0];
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan1)[0].get().getSinkDescriptor()->getSinkType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan1)[0].get().getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(flatten(sourcePlan1).size(), 3);

    const auto sourcePlan2 = plan[GrpcAddr("source-node:8080")][1];
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan2)[0].get().getSinkDescriptor()->getSinkType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan2)[0].get().getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(flatten(sourcePlan2).size(), 4);
}

TEST_F(QueryManagerTest, FourWayJoin)
{
    auto [ctx, boundPlan] = loadAndBind("four_way_join.yaml");
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    for (const auto& [node, plans] : plan)
    {
        for (const auto& localPlan : plans)
        {
            NES_DEBUG("Plan on node {}: \n{}", node, localPlan);
        }
    }
}

TEST_F(QueryManagerTest, BridgePlacement)
{
    auto [ctx, boundPlan] = loadAndBind("bridge.yaml");
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto sourcePlan = plan[GrpcAddr("source-node:8080")].front();
    EXPECT_EQ(flatten(sourcePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan)[0].get().getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan)[0].get().getSinkDescriptor()->getSinkType(), "Network");

    const auto intermediatePlan = plan[GrpcAddr("intermediate-node:8080")].front();
    EXPECT_EQ(flatten(intermediatePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan)[0].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan)[0].get().getSinkDescriptor()->getSinkType(), "Network");

    const auto sinkPlan = plan[GrpcAddr("sink-node:8080")].front();
    EXPECT_EQ(flatten(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].get().getSinkDescriptor()->getSinkType(), "Void");
}

TEST_F(QueryManagerTest, LongBridgePlacement)
{
    auto [ctx, boundPlan] = loadAndBind("long_bridge.yaml");
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto sourcePlan = plan[GrpcAddr("source-node:8080")].front();
    EXPECT_EQ(flatten(sourcePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan)[0].get().getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan)[0].get().getSinkDescriptor()->getSinkType(), "Network");

    const auto intermediatePlan0 = plan[GrpcAddr("intermediate-node0:8080")].front();
    EXPECT_EQ(flatten(intermediatePlan0).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan0).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan0)[0].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan0).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan0)[0].get().getSinkDescriptor()->getSinkType(), "Network");

    const auto intermediatePlan1 = plan[GrpcAddr("intermediate-node1:8080")].front();
    EXPECT_EQ(flatten(intermediatePlan1).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan1).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan1)[0].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan1)[0].get().getSinkDescriptor()->getSinkType(), "Network");

    const auto sinkPlan = plan[GrpcAddr("sink-node:8080")].front();
    EXPECT_EQ(flatten(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].get().getSinkDescriptor()->getSinkType(), "Void");
}

TEST_F(QueryManagerTest, BridgePlacementJoin)
{
    auto [ctx, boundPlan] = loadAndBind("bridge_join.yaml");
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto sourcePlan1 = plan[GrpcAddr("source-node:8080")][0];
    EXPECT_EQ(flatten(sourcePlan1).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan1)[0].get().getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan1)[0].get().getSinkDescriptor()->getSinkType(), "Network");

    const auto sourcePlan2 = plan[GrpcAddr("source-node:8080")][1];
    EXPECT_EQ(flatten(sourcePlan2).size(), 4);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan2)[0].get().getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan2)[0].get().getSinkDescriptor()->getSinkType(), "Network");

    const auto intermediatePlan1 = plan[GrpcAddr("intermediate-node:8080")][0];
    EXPECT_EQ(flatten(intermediatePlan1).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan1).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan1)[0].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan1)[0].get().getSinkDescriptor()->getSinkType(), "Network");

    const auto intermediatePlan2 = plan[GrpcAddr("intermediate-node:8080")][1];
    EXPECT_EQ(flatten(intermediatePlan2).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan2).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan2)[0].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan2)[0].get().getSinkDescriptor()->getSinkType(), "Network");

    const auto sinkPlan = plan[GrpcAddr("sink-node:8080")].front();
    EXPECT_EQ(flatten(sinkPlan).size(), 4);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[1].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].get().getSinkDescriptor()->getSinkType(), "Void");
}

TEST_F(QueryManagerTest, ComplexJoinQuery)
{
    auto [ctx, boundPlan] = loadAndBind("complex_join.yaml");
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    for (const auto& [node, localPlans] : plan)
    {
        for (const auto& localPlan : localPlans)
        {
            NES_DEBUG("Plan on node {}: \n{}", node, localPlan);
        }
    }
    const auto sourceNode0Plan = plan[GrpcAddr("source-node0:8080")].front();
    EXPECT_EQ(flatten(sourceNode0Plan).size(), 6);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode0Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode0Plan)[0].get().getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode0Plan)[1].get().getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sourceNode0Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourceNode0Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode0Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode0Plan)[0].get().getSinkDescriptor()->getSinkType(), "Network");

    const auto sourceNode1Plan = plan[GrpcAddr("source-node1:8080")].front();
    EXPECT_EQ(flatten(sourceNode1Plan).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan)[0].get().getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan)[0].get().getSinkDescriptor()->getSinkType(), "Network");

    const auto sourceNode2Plan = plan[GrpcAddr("source-node2:8080")].front();
    EXPECT_EQ(flatten(sourceNode2Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan)[0].get().getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan)[0].get().getSinkDescriptor()->getSinkType(), "Network");

    const auto intermediateNode0Plan = plan[GrpcAddr("intermediate-node0:8080")].front();
    EXPECT_EQ(flatten(intermediateNode0Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode0Plan).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode0Plan)[0].get().getSourceDescriptor().getSourceType(),
        "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode0Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode0Plan)[0].get().getSinkDescriptor()->getSinkType(), "Network");

    const auto intermediateNode1Plan = plan[GrpcAddr("intermediate-node1:8080")].front();
    EXPECT_EQ(flatten(intermediateNode1Plan).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode1Plan).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode1Plan)[0].get().getSourceDescriptor().getSourceType(),
        "Network");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(intermediateNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode1Plan)[0].get().getSinkDescriptor()->getSinkType(), "Network");

    const auto sinkPlan = plan[GrpcAddr("sink-node:8080")].front();
    EXPECT_EQ(flatten(sinkPlan).size(), 6);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[1].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[2].get().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].get().getSinkDescriptor()->getSinkType(), "Void");
}

TEST_F(QueryManagerTest, Disconnected)
{
    auto [ctx, boundPlan] = loadAndBind("disconnected.yaml");
    EXPECT_ANY_THROW(QueryPlanner::with(ctx).plan(std::move(boundPlan)));
}

TEST_F(QueryManagerTest, NotEnoughCapacities)
{
    auto [ctx, boundPlan] = loadAndBind("not_enough_capacities.yaml");
    EXPECT_ANY_THROW(QueryPlanner::with(ctx).plan(std::move(boundPlan)));
}

TEST_F(QueryManagerTest, MultiplePhysicalSources)
{
    /// Plan has a logical source that is referenced by 9 physical sources. 4 physical source on source and 5 on source2.
    /// Capacity prevents the union from beeing placed at the intermediate node
    auto [ctx, boundPlan] = loadAndBind("multiple_physical_sources.yaml");
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto sinkPlans = plan[GrpcAddr("sink-node:8080")];
    ASSERT_EQ(sinkPlans.size(), 1);
    EXPECT_EQ(flatten(sinkPlans.front()).size(), 9 + 1 + 1);

    const auto intermediatePlans = plan[GrpcAddr("intermediate-node:8080")];
    ASSERT_EQ(intermediatePlans.size(), 9);
    EXPECT_EQ(flatten(intermediatePlans.front()).size(), 2);

    const auto source2Plans = plan[GrpcAddr("source-node2:8080")];
    ASSERT_EQ(source2Plans.size(), 5);
    EXPECT_EQ(flatten(source2Plans.front()).size(), 2);

    const auto sourcePlans = plan[GrpcAddr("source-node:8080")];
    ASSERT_EQ(sourcePlans.size(), 4);
    EXPECT_EQ(flatten(source2Plans.front()).size(), 2);
}

}
