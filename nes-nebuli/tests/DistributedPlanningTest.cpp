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
#include <YAML/YamlBinder.hpp>
#include <YAML/YamlLoader.hpp>

#include <BaseUnitTest.hpp>
#include <QueryConfig.hpp>
#include <QueryPlanning.hpp>

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

TEST_F(DistributedPlanningTest, BasicPlacementSingleNode)
{
    auto queryConfig = CLI::YamlLoader<CLI::QueryConfig>::load(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "basic_single_node.yaml");
    auto parsedPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryConfig.query);
    auto [boundPlan, ctx] = CLI::YamlBinder::from(queryConfig).bind(std::move(parsedPlan));
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const LogicalPlan localPlan = plan["localhost:8081"].front();
    const auto root = localPlan.getRootOperators();
    EXPECT_TRUE(root.size() == 1);
    const auto sink = root.back().tryGet<SinkLogicalOperator>();
    EXPECT_TRUE(sink.has_value());
    EXPECT_EQ(sink->getSinkDescriptor()->getSinkType(), "File");
    const auto leaf = getLeafOperators(localPlan);
    EXPECT_TRUE(leaf.size() == 1);
    const auto source = leaf.back().tryGet<SourceDescriptorLogicalOperator>();
    EXPECT_TRUE(source.has_value());
    EXPECT_EQ(source->getSourceDescriptor().getSourceType(), "File");
}

TEST_F(DistributedPlanningTest, BasicPlacementTwoNodes)
{
    auto queryConfig = CLI::YamlLoader<CLI::QueryConfig>::load(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "basic_two_nodes.yaml");
    auto parsedPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryConfig.query);
    auto [boundPlan, ctx] = CLI::YamlBinder::from(std::move(queryConfig)).bind(std::move(parsedPlan));
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto sourceNodePlan = plan["source-node"].front();
    const auto root = sourceNodePlan.getRootOperators();
    EXPECT_TRUE(root.size() == 1);
    const auto networkSink = root.back().tryGet<SinkLogicalOperator>();
    EXPECT_TRUE(networkSink.has_value());
    EXPECT_EQ(networkSink.value().getSinkDescriptor()->getSinkType(), "Network");
    const auto sources = getOperatorByType<SourceDescriptorLogicalOperator>(sourceNodePlan);
    EXPECT_TRUE(sources.size() == 1);
    EXPECT_EQ(sources.front().getSourceDescriptor().getSourceType(), "File");

    const auto sinkNodePlan = plan["sink-node"].front();
    const auto leaf = getLeafOperators(sinkNodePlan);
    EXPECT_TRUE(leaf.size() == 1);
    const auto networkSource = leaf.back().tryGet<SourceDescriptorLogicalOperator>();
    EXPECT_TRUE(networkSource.has_value());
    EXPECT_EQ(networkSource.value().getSourceDescriptor().getSourceType(), "Network");
}

TEST_F(DistributedPlanningTest, JoinPlacementWithOneSelection)
{
    auto queryConfig
        = CLI::YamlLoader<CLI::QueryConfig>::load(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "join_with_one_selection.yaml");
    auto parsedPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryConfig.query);
    auto [boundPlan, ctx] = CLI::YamlBinder::from(std::move(queryConfig)).bind(std::move(parsedPlan));
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto sourceNode0Plan = plan["source-node0"].front();
    EXPECT_EQ(flatten(sourceNode0Plan).size(), 3);
    EXPECT_EQ(getLeafOperators(sourceNode0Plan).size(), 1);
    EXPECT_EQ(
        getLeafOperators(sourceNode0Plan)
            .front()
            .get<SourceDescriptorLogicalOperator>()
            .getSourceDescriptor()
            .getLogicalSource()
            .getLogicalSourceName(),
        "stream0");
    EXPECT_EQ(sourceNode0Plan.getRootOperators().size(), 1);
    EXPECT_EQ(sourceNode0Plan.getRootOperators().front().get<SinkLogicalOperator>().getSinkDescriptor()->getSinkType(), "Network");

    const auto sourceNode1Plan = plan["source-node1"].front();
    EXPECT_EQ(flatten(sourceNode1Plan).size(), 4);
    EXPECT_EQ(getLeafOperators(sourceNode1Plan).size(), 1);
    EXPECT_EQ(
        getLeafOperators(sourceNode1Plan)
            .front()
            .get<SourceDescriptorLogicalOperator>()
            .getSourceDescriptor()
            .getLogicalSource()
            .getLogicalSourceName(),
        "stream1");
    EXPECT_EQ(sourceNode1Plan.getRootOperators().size(), 1);
    EXPECT_EQ(sourceNode1Plan.getRootOperators().front().get<SinkLogicalOperator>().getSinkDescriptor()->getSinkType(), "Network");
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(sourceNode1Plan).size(), 1);

    const auto sinkNodePlan = plan["sink-node"].front();
    EXPECT_EQ(flatten(sinkNodePlan).size(), 4);
    EXPECT_EQ(getLeafOperators(sinkNodePlan).size(), 2);
    EXPECT_EQ(getLeafOperators(sinkNodePlan)[0].get<SourceDescriptorLogicalOperator>().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getLeafOperators(sinkNodePlan)[1].get<SourceDescriptorLogicalOperator>().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(sinkNodePlan.getRootOperators().size(), 1);
    EXPECT_EQ(sinkNodePlan.getRootOperators().front().get<SinkLogicalOperator>().getSinkName(), "sink");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkNodePlan).size(), 1);
}

TEST_F(DistributedPlanningTest, PlacementWithThreeNodes)
{
    auto queryConfig = CLI::YamlLoader<CLI::QueryConfig>::load(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "three_nodes.yaml");
    auto parsedPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryConfig.query);
    auto [boundPlan, ctx] = CLI::YamlBinder::from(std::move(queryConfig)).bind(std::move(parsedPlan));
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto plan0 = plan["sink-node"].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[1].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(flatten(plan0).size(), 4);

    const auto plan1 = plan["source-node0"].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).front().getSinkDescriptor()->getSinkType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<ProjectionLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(flatten(plan1).size(), 5);

    const auto plan2 = plan["source-node1"].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).front().getSinkDescriptor()->getSinkType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(flatten(plan2).size(), 4);
}

TEST_F(DistributedPlanningTest, JoinPlacementWithLimitedCapacity)
{
    auto queryConfig
        = CLI::YamlLoader<CLI::QueryConfig>::load(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "join_with_limited_capacity.yaml");
    auto parsedPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryConfig.query);
    auto [boundPlan, ctx] = CLI::YamlBinder::from(std::move(queryConfig)).bind(std::move(parsedPlan));
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto plan0 = plan["sink-node"].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[1].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(flatten(plan0).size(), 5);

    const auto plan1 = plan["source-node0"].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).front().getSinkDescriptor()->getSinkType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(flatten(plan1).size(), 3);

    const auto plan2 = plan["source-node1"].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).front().getSinkDescriptor()->getSinkType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(flatten(plan2).size(), 3);
}

TEST_F(DistributedPlanningTest, JoinPlacementWithLimitedCapacityOnTwoNodes)
{
    auto queryConfig = CLI::YamlLoader<CLI::QueryConfig>::load(
        std::filesystem::path{NEBULI_TEST_DATA_DIR} / "join_with_limited_capacity_on_two_nodes.yaml");
    auto parsedPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryConfig.query);
    auto [boundPlan, ctx] = CLI::YamlBinder::from(std::move(queryConfig)).bind(std::move(parsedPlan));
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto sinkPlan = plan["sink-node"].front();
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).front().getSinkDescriptor()->getSinkType(), "Print");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(flatten(sinkPlan).size(), 4);

    const auto sourcePlan1 = plan["source-node"][0];
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan1)[0].getSinkDescriptor()->getSinkType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan1)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(flatten(sourcePlan1).size(), 3);

    const auto sourcePlan2 = plan["source-node"][1];
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan2)[0].getSinkDescriptor()->getSinkType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan2)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(flatten(sourcePlan2).size(), 4);
}

TEST_F(DistributedPlanningTest, FourWayJoin)
{
    auto queryConfig = CLI::YamlLoader<CLI::QueryConfig>::load(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "four_way_join.yaml");
    auto parsedPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryConfig.query);
    auto [boundPlan, ctx] = CLI::YamlBinder::from(std::move(queryConfig)).bind(std::move(parsedPlan));
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

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
    auto queryConfig = CLI::YamlLoader<CLI::QueryConfig>::load(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "bridge.yaml");
    auto parsedPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryConfig.query);
    auto [boundPlan, ctx] = CLI::YamlBinder::from(std::move(queryConfig)).bind(std::move(parsedPlan));
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto sourcePlan = plan["source-node"].front();
    EXPECT_EQ(flatten(sourcePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan)[0].getSinkDescriptor()->getSinkType(), "Network");

    const auto intermediatePlan = plan["intermediate-node"].front();
    EXPECT_EQ(flatten(intermediatePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan)[0].getSinkDescriptor()->getSinkType(), "Network");

    const auto sinkPlan = plan["sink-node"].front();
    EXPECT_EQ(flatten(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].getSinkDescriptor()->getSinkType(), "Print");
}

TEST_F(DistributedPlanningTest, LongBridgePlacement)
{
    auto queryConfig = CLI::YamlLoader<CLI::QueryConfig>::load(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "long_bridge.yaml");
    auto parsedPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryConfig.query);
    auto [boundPlan, ctx] = CLI::YamlBinder::from(std::move(queryConfig)).bind(std::move(parsedPlan));
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto sourcePlan = plan["source-node"].front();
    EXPECT_EQ(flatten(sourcePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan)[0].getSinkDescriptor()->getSinkType(), "Network");

    const auto intermediatePlan0 = plan["intermediate-node0"].front();
    EXPECT_EQ(flatten(intermediatePlan0).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan0).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan0)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan0).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan0)[0].getSinkDescriptor()->getSinkType(), "Network");

    const auto intermediatePlan1 = plan["intermediate-node1"].front();
    EXPECT_EQ(flatten(intermediatePlan1).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan1)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan1)[0].getSinkDescriptor()->getSinkType(), "Network");

    const auto sinkPlan = plan["sink-node"].front();
    EXPECT_EQ(flatten(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].getSinkDescriptor()->getSinkType(), "Print");
}

TEST_F(DistributedPlanningTest, BridgePlacementJoin)
{
    auto queryConfig = CLI::YamlLoader<CLI::QueryConfig>::load(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "bridge_join.yaml");
    auto parsedPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryConfig.query);
    auto [boundPlan, ctx] = CLI::YamlBinder::from(std::move(queryConfig)).bind(std::move(parsedPlan));
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    const auto sourcePlan1 = plan["source-node"][0];
    EXPECT_EQ(flatten(sourcePlan1).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan1)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan1)[0].getSinkDescriptor()->getSinkType(), "Network");

    const auto sourcePlan2 = plan["source-node"][1];
    EXPECT_EQ(flatten(sourcePlan2).size(), 4);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan2)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan2)[0].getSinkDescriptor()->getSinkType(), "Network");

    const auto intermediatePlan1 = plan["intermediate-node"][0];
    EXPECT_EQ(flatten(intermediatePlan1).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan1)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan1)[0].getSinkDescriptor()->getSinkType(), "Network");

    const auto intermediatePlan2 = plan["intermediate-node"][1];
    EXPECT_EQ(flatten(intermediatePlan2).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan2)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan2)[0].getSinkDescriptor()->getSinkType(), "Network");

    const auto sinkPlan = plan["sink-node"].front();
    EXPECT_EQ(flatten(sinkPlan).size(), 4);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[1].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].getSinkDescriptor()->getSinkType(), "Print");
}

TEST_F(DistributedPlanningTest, ComplexJoinQuery)
{
    auto queryConfig = CLI::YamlLoader<CLI::QueryConfig>::load(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "complex_join.yaml");
    auto parsedPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryConfig.query);
    auto [boundPlan, ctx] = CLI::YamlBinder::from(std::move(queryConfig)).bind(std::move(parsedPlan));
    auto plan = QueryPlanner::with(ctx).plan(std::move(boundPlan));

    for (const auto& [node, localPlans] : plan)
    {
        for (const auto& localPlan : localPlans)
        {
            NES_DEBUG("Plan on node {}: \n{}", node, localPlan);
        }
    }
    const auto sourceNode0Plan = plan["source-node0"].front();
    EXPECT_EQ(flatten(sourceNode0Plan).size(), 6);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode0Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode0Plan)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode0Plan)[1].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sourceNode0Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourceNode0Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode0Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode0Plan)[0].getSinkDescriptor()->getSinkType(), "Network");

    const auto sourceNode1Plan = plan["source-node1"].front();
    EXPECT_EQ(flatten(sourceNode1Plan).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan)[0].getSinkDescriptor()->getSinkType(), "Network");

    const auto sourceNode2Plan = plan["source-node2"].front();
    EXPECT_EQ(flatten(sourceNode2Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan)[0].getSinkDescriptor()->getSinkType(), "Network");

    const auto intermediateNode0Plan = plan["intermediate-node0"].front();
    EXPECT_EQ(flatten(intermediateNode0Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode0Plan).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode0Plan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode0Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode0Plan)[0].getSinkDescriptor()->getSinkType(), "Network");

    const auto intermediateNode1Plan = plan["intermediate-node1"].front();
    EXPECT_EQ(flatten(intermediateNode1Plan).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode1Plan).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode1Plan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(intermediateNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode1Plan)[0].getSinkDescriptor()->getSinkType(), "Network");

    const auto sinkPlan = plan["sink-node"].front();
    EXPECT_EQ(flatten(sinkPlan).size(), 6);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[1].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[2].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].getSinkDescriptor()->getSinkType(), "Print");
}

}
