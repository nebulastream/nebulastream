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

#include <Distributed/OperatorPlacement.hpp>
#include <Distributed/QueryDecomposition.hpp>
#include <LegacyOptimizer/LegacyOptimizer.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <YAML/YAMLBinder.hpp>
#include <BaseUnitTest.hpp>
#include <QueryConfig.hpp>

namespace NES
{

static constexpr auto TEST_DATA_DIR = "/tmp/nebulastream/nes-nebuli/src/Distributed/testdata";

class DistributedPlanningTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("DistributedPlanning.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup DistributedPlanning class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }

    static QueryDecomposer::DecomposedLogicalPlan planQueryFromTestYAML(const std::filesystem::path& pathToQueryConfig)
    {
        CLI::QueryConfig queryConfig = CLI::YAMLLoader::load(pathToQueryConfig);
        auto [plan, topology, nodeCatalog, sourceCatalog, sinkCatalog] = CLI::YAMLBinder::from(std::move(queryConfig)).bind();
        LegacyOptimizer::OptimizedLogicalPlan optimizedPlan = LegacyOptimizer::optimize(std::move(plan), sourceCatalog);
        OperatorPlacer::PlacedLogicalPlan placedPlan
            = OperatorPlacer::from(std::move(optimizedPlan), topology, nodeCatalog, sinkCatalog).place();
        QueryDecomposer::DecomposedLogicalPlan decomposedPlan
            = QueryDecomposer::from(
                  std::move(placedPlan), topology, sourceCatalog, sinkCatalog)
                  .decompose();
        return decomposedPlan;
    }

    static void TearDownTestSuite() { NES_INFO("Tear down DistributedPlanning class."); }
};

TEST_F(DistributedPlanningTest, BasicPlacementSingleNode)
{
    const QueryDecomposer::DecomposedLogicalPlan plan
        = planQueryFromTestYAML(std::filesystem::path{TEST_DATA_DIR} / "basic_placement_single_node.yaml");
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
        = planQueryFromTestYAML(std::filesystem::path{TEST_DATA_DIR} / "basic_placement_two_nodes.yaml");
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
    const auto decomposedPlan = planQueryFromTestYAML(std::filesystem::path{TEST_DATA_DIR} / "join_placement_with_one_selection.yaml");

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
    const auto decomposedPlan = planQueryFromTestYAML(std::filesystem::path{TEST_DATA_DIR} / "placement_with_three_nodes.yaml");

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
    const auto decomposedPlan = planQueryFromTestYAML(std::filesystem::path{TEST_DATA_DIR} / "join_placement_with_limited_capacity.yaml");

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
        = planQueryFromTestYAML(std::filesystem::path{TEST_DATA_DIR} / "join_placement_with_limited_capacity_on_two_nodes.yaml");

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
    const auto decomposedPlan = planQueryFromTestYAML(std::filesystem::path{TEST_DATA_DIR} / "bridge_placement.yaml");

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
    const auto decomposedPlan = planQueryFromTestYAML(std::filesystem::path{TEST_DATA_DIR} / "long_bridge_placement.yaml");

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
    const auto decomposedPlan = planQueryFromTestYAML(std::filesystem::path{TEST_DATA_DIR} / "bridge_placement_join.yaml");

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

//                                  SINK(sink)
//                                      │
//                               Join(id3 = id4)
//               ┌──────────────────────┴──────────────┐
//       Join(id2 = id3)                WATERMARK_ASSIGNER(Event time)
//         ┌─────┴─────────────────────┐               └────────────┐
//  Join(id = id2)      WATERMARK_ASSIGNER(Event time)      SOURCE(stream4)
//         └─────┬─────────────────────┴────────┬──────────────────────┐
//WATERMARK_ASSIGNER(Event time) WATERMARK_ASSIGNER(Event time) SOURCE(stream3)
//               └──┐                           └──────────┐
//           SOURCE(stream)                        SOURCE(stream2)
TEST_F(DistributedPlanningTest, ComplexJoinQuery)
{
    // TopologyGraph topology;
    // const std::string n1 = "1";
    // const std::string n2 = "2";
    // const std::string n3 = "3";
    // const std::string n4 = "4";
    // const std::string n5 = "5";
    // const std::string n6 = "6";
    // topology.addNode(n1);
    // topology.addNode(n2);
    // topology.addNode(n3);
    // topology.addNode(n4);
    // topology.addNode(n5);
    // topology.addNode(n6);
    // topology.addDownstreamNode(n1, n6);
    // topology.addDownstreamNode(n2, n4);
    // topology.addDownstreamNode(n3, n5);
    // topology.addDownstreamNode(n4, n6);
    // topology.addDownstreamNode(n5, n6);
    //
    // sourcePlacement.try_emplace("stream", n1);
    // sourcePlacement.try_emplace("stream2", n1);
    // sourcePlacement.try_emplace("stream3", n2);
    // sourcePlacement.try_emplace("stream4", n3);
    // sinkPlacement.try_emplace("sink", n6);
    // const auto nodeCatalog = createNodeCatalog(n1, n2, n3, n4, n5, n6);
    //
    // auto initialPlan = getPlanFromSQL(
    //     "SELECT * FROM (SELECT * FROM stream) INNER JOIN (SELECT * FROM stream2) ON id = id2 WINDOW TUMBLING (ts, size 1 sec)"
    //     "INNER JOIN (SELECT * FROM stream3) ON id2 = id3 WINDOW TUMBLING (ts, size 1 sec) "
    //     "INNER JOIN (SELECT * FROM stream4) ON id3 = id4 WINDOW TUMBLING (ts, size 1 sec) INTO sink");
    //
    // Capacities capacities = {{n1, 3}, {n2, 1}, {n3, 0}, {n4, 0}, {n5, 1}, {n6, 2}};
    const auto decomposedPlan = planQueryFromTestYAML(std::filesystem::path{TEST_DATA_DIR} / "complex_join_query.yaml");
    for (const auto& [node, plan] : decomposedPlan)
    {
        NES_DEBUG("Plan on node {}: \n{}", node, plan);
    }
}

}
