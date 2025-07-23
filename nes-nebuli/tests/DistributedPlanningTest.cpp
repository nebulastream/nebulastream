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
#include <utility>

#include <gtest/gtest.h>

#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <BaseUnitTest.hpp>
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
    const QueryDecomposer::DecomposedLogicalPlan plan
        = QueryPlanner::plan(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "basic_placement_single_node.yaml");
    const LogicalPlan localPlan = plan.at("0.0.0.0:15054");
    const auto root = localPlan.getRootOperators();
    EXPECT_TRUE(root.size() == 1);
    const auto sink = root.back().tryGet<SinkLogicalOperator>();
    EXPECT_TRUE(sink.has_value());
    EXPECT_EQ(sink->sinkDescriptor->sinkType, "File");
    const auto leaf = getLeafOperators(localPlan);
    EXPECT_TRUE(leaf.size() == 1);
    const auto source = leaf.back().tryGet<SourceDescriptorLogicalOperator>();
    EXPECT_TRUE(source.has_value());
    EXPECT_EQ(source->getSourceDescriptor().getSourceType(), "File");
}

TEST_F(DistributedPlanningTest, BasicPlacementTwoNodes)
{
    const QueryDecomposer::DecomposedLogicalPlan plan
        = QueryPlanner::plan(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "basic_placement_two_nodes.yaml");
    const auto sourceNodePlan = plan.at("source-node");
    const auto root = sourceNodePlan.getRootOperators();
    EXPECT_TRUE(root.size() == 1);
    const auto networkSink = root.back().tryGet<SinkLogicalOperator>();
    EXPECT_TRUE(networkSink.has_value());
    EXPECT_EQ(networkSink.value().sinkDescriptor->sinkType, "Network");

    const auto sinkNodePlan = plan.at("sink-node");
    const auto leaf = getLeafOperators(sinkNodePlan);
    EXPECT_TRUE(leaf.size() == 1);
    const auto networkSource = leaf.back().tryGet<SourceDescriptorLogicalOperator>();
    EXPECT_TRUE(networkSource.has_value());
    EXPECT_EQ(networkSource.value().getSourceDescriptor().getSourceType(), "Network");
}

TEST_F(DistributedPlanningTest, JoinPlacementWithOneSelection)
{
    const auto decomposedPlan = QueryPlanner::plan(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "join_placement_with_one_selection.yaml");

    const auto sourceNode0Plan = decomposedPlan.at("source-node0");
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
    EXPECT_EQ(sourceNode0Plan.getRootOperators().front().get<SinkLogicalOperator>().sinkDescriptor->sinkType, "Network");

    const auto sourceNode1Plan = decomposedPlan.at("source-node1");
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
    EXPECT_EQ(sourceNode1Plan.getRootOperators().front().get<SinkLogicalOperator>().sinkDescriptor->sinkType, "Network");
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(sourceNode1Plan).size(), 1);

    const auto sinkNodePlan = decomposedPlan.at("sink-node");
    EXPECT_EQ(flatten(sinkNodePlan).size(), 4);
    EXPECT_EQ(getLeafOperators(sinkNodePlan).size(), 2);
    EXPECT_EQ(getLeafOperators(sinkNodePlan)[0].get<SourceDescriptorLogicalOperator>().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getLeafOperators(sinkNodePlan)[1].get<SourceDescriptorLogicalOperator>().getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(sinkNodePlan.getRootOperators().size(), 1);
    EXPECT_EQ(sinkNodePlan.getRootOperators().front().get<SinkLogicalOperator>().sinkName, "sink");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkNodePlan).size(), 1);
}

TEST_F(DistributedPlanningTest, PlacementWithThreeNodes)
{
    const auto decomposedPlan = QueryPlanner::plan(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "placement_with_three_nodes.yaml");

    const auto plan0 = decomposedPlan.at("sink-node");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[1].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(flatten(plan0).size(), 4);

    const auto plan1 = decomposedPlan.at("source-node0");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).front().sinkDescriptor->sinkType, "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<ProjectionLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(flatten(plan1).size(), 5);

    const auto plan2 = decomposedPlan.at("source-node1");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).front().sinkDescriptor->sinkType, "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(flatten(plan2).size(), 4);
}

TEST_F(DistributedPlanningTest, JoinPlacementWithLimitedCapacity)
{
    const auto decomposedPlan
        = QueryPlanner::plan(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "join_placement_with_limited_capacity.yaml");

    const auto plan0 = decomposedPlan.at("sink-node");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan0)[1].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan0).size(), 1);
    EXPECT_EQ(flatten(plan0).size(), 5);

    const auto plan1 = decomposedPlan.at("source-node0");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan1).front().sinkDescriptor->sinkType, "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan1).size(), 1);
    EXPECT_EQ(flatten(plan1).size(), 3);

    const auto plan2 = decomposedPlan.at("source-node1");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(plan2).front().sinkDescriptor->sinkType, "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(plan2).size(), 1);
    EXPECT_EQ(flatten(plan2).size(), 3);
}

TEST_F(DistributedPlanningTest, JoinPlacementWithLimitedCapacityOnTwoNodes)
{
    const auto decomposedPlan
        = QueryPlanner::plan(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "join_placement_with_limited_capacity_on_two_nodes.yaml");

    const auto sinkPlan = decomposedPlan.at("sink-node");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).front().sinkDescriptor->sinkType, "Print");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(flatten(sinkPlan).size(), 4);

    const auto sourcePlan = decomposedPlan.at("source-node");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan)[0].sinkDescriptor->sinkType, "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan)[1].sinkDescriptor->sinkType, "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourcePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(flatten(sourcePlan).size(), 7);
}

TEST_F(DistributedPlanningTest, BridgePlacement)
{
    const auto decomposedPlan = QueryPlanner::plan(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "bridge_placement.yaml");

    const auto sourcePlan = decomposedPlan.at("source-node");
    EXPECT_EQ(flatten(sourcePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan)[0].sinkDescriptor->sinkType, "Network");

    const auto intermediatePlan = decomposedPlan.at("intermediate-node");
    EXPECT_EQ(flatten(intermediatePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan)[0].sinkDescriptor->sinkType, "Network");

    const auto sinkPlan = decomposedPlan.at("sink-node");
    EXPECT_EQ(flatten(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].sinkDescriptor->sinkType, "Print");
}

TEST_F(DistributedPlanningTest, LongBridgePlacement)
{
    const auto decomposedPlan = QueryPlanner::plan(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "long_bridge_placement.yaml");

    const auto sourcePlan = decomposedPlan.at("source-node");
    EXPECT_EQ(flatten(sourcePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan)[0].sinkDescriptor->sinkType, "Network");

    const auto intermediatePlan0 = decomposedPlan.at("intermediate-node0");
    EXPECT_EQ(flatten(intermediatePlan0).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan0).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan0)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan0).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan0)[0].sinkDescriptor->sinkType, "Network");

    const auto intermediatePlan1 = decomposedPlan.at("intermediate-node1");
    EXPECT_EQ(flatten(intermediatePlan1).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan1)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan1).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan1)[0].sinkDescriptor->sinkType, "Network");

    const auto sinkPlan = decomposedPlan.at("sink-node");
    EXPECT_EQ(flatten(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].sinkDescriptor->sinkType, "Print");
}

TEST_F(DistributedPlanningTest, BridgePlacementJoin)
{
    const auto decomposedPlan = QueryPlanner::plan(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "bridge_placement_join.yaml");

    const auto sourcePlan = decomposedPlan.at("source-node");
    EXPECT_EQ(flatten(sourcePlan).size(), 7);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourcePlan)[1].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan)[0].sinkDescriptor->sinkType, "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourcePlan)[1].sinkDescriptor->sinkType, "Network");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourcePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SelectionLogicalOperator>(sourcePlan).size(), 1);

    const auto intermediatePlan = decomposedPlan.at("intermediate-node");
    EXPECT_EQ(flatten(intermediatePlan).size(), 4);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediatePlan)[1].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan)[0].sinkDescriptor->sinkType, "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediatePlan)[1].sinkDescriptor->sinkType, "Network");

    const auto sinkPlan = decomposedPlan.at("sink-node");
    EXPECT_EQ(flatten(sinkPlan).size(), 4);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[1].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].sinkDescriptor->sinkType, "Print");
}

TEST_F(DistributedPlanningTest, ComplexJoinQuery)
{
    const auto decomposedPlan = QueryPlanner::plan(std::filesystem::path{NEBULI_TEST_DATA_DIR} / "complex_join_query.yaml");
    for (const auto& [node, plan] : decomposedPlan)
    {
        NES_DEBUG("Plan on node {}: \n{}", node, plan);
    }
    const auto sourceNode0Plan = decomposedPlan.at("source-node0");
    EXPECT_EQ(flatten(sourceNode0Plan).size(), 6);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode0Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode0Plan)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode0Plan)[1].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sourceNode0Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourceNode0Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode0Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode0Plan)[0].sinkDescriptor->sinkType, "Network");

    const auto sourceNode1Plan = decomposedPlan.at("source-node1");
    EXPECT_EQ(flatten(sourceNode1Plan).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan)[0].sinkDescriptor->sinkType, "Network");

    const auto sourceNode2Plan = decomposedPlan.at("source-node2");
    EXPECT_EQ(flatten(sourceNode2Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sourceNode1Plan)[0].getSourceDescriptor().getSourceType(), "File");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sourceNode1Plan)[0].sinkDescriptor->sinkType, "Network");

    const auto intermediateNode0Plan = decomposedPlan.at("intermediate-node0");
    EXPECT_EQ(flatten(intermediateNode0Plan).size(), 2);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode0Plan).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode0Plan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode0Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode0Plan)[0].sinkDescriptor->sinkType, "Network");

    const auto intermediateNode1Plan = decomposedPlan.at("intermediate-node1");
    EXPECT_EQ(flatten(intermediateNode1Plan).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode1Plan).size(), 1);
    EXPECT_EQ(
        getOperatorByType<SourceDescriptorLogicalOperator>(intermediateNode1Plan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(intermediateNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode1Plan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(intermediateNode1Plan)[0].sinkDescriptor->sinkType, "Network");

    const auto sinkPlan = decomposedPlan.at("sink-node");
    EXPECT_EQ(flatten(sinkPlan).size(), 6);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan).size(), 3);
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[0].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[1].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<SourceDescriptorLogicalOperator>(sinkPlan)[2].getSourceDescriptor().getSourceType(), "Network");
    EXPECT_EQ(getOperatorByType<JoinLogicalOperator>(sinkPlan).size(), 2);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan).size(), 1);
    EXPECT_EQ(getOperatorByType<SinkLogicalOperator>(sinkPlan)[0].sinkDescriptor->sinkType, "Print");
}

}
