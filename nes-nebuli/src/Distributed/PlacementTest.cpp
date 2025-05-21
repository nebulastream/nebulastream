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

#include <Distributed/Placement.hpp>

#include <memory>
#include <AntlrSQLParser.h>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SourceCatalogs/LogicalSource.hpp>
#include <SourceCatalogs/PhysicalSource.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <Util/Ranges.hpp>
#include <BaseUnitTest.hpp>
#include <NebuLI.hpp>

namespace NES
{

class PlacementTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("PlacementTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup PlacementTest class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        sourceCatalog = {};
        sinkCatalog = {};
    }

    static void TearDownTestSuite() { NES_INFO("Tear down PlacementTest class."); }

    std::shared_ptr<QueryPlan> getQuery(std::string_view queryString)
    {
        auto realSourceCatalog = std::make_shared<NES::Catalogs::Source::SourceCatalog>();


        /// Add logical sources to the SourceCatalog to prepare adding physical sources to each logical source.
        for (const auto& [logicalSourceName, _] : sourceCatalog | std::views::values)
        {
            auto schema = Schema::create();
            realSourceCatalog->addLogicalSource(logicalSourceName, schema);
        }

        /// Add physical sources to corresponding logical sources.
        for (const auto& [physicalSourceName, v] : sourceCatalog)
        {
            auto [logicalSourceName, _] = v;
            auto logicalSource = realSourceCatalog->getLogicalSource(logicalSourceName);
            realSourceCatalog->addPhysicalSource(
                logicalSourceName,
                Catalogs::Source::SourceCatalogEntry::create(
                    NES::PhysicalSource::create(Sources::SourceDescriptor{
                        logicalSource->getSchema(),
                        logicalSourceName,
                        "TEST",
                        64,
                        Sources::ParserConfig{},
                        Configurations::DescriptorConfig::Config{}}),
                    realSourceCatalog->getLogicalSource(logicalSourceName),
                    INITIAL<Distributed::Topology::Node>));
        }

        auto logicalSourceExpansionRule = NES::Optimizer::LogicalSourceExpansionRule(realSourceCatalog);
        auto originIdInferencePhase = Optimizer::OriginIdInferencePhase::create();

        auto query = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(queryString);
        logicalSourceExpansionRule.apply(query);
        originIdInferencePhase->execute(query);


        auto sink = query->getSinkOperators().at(0);
        auto sinkPlacement = sinkCatalog.at(sink->sinkName);
        sink->addProperty(std::string(Distributed::Placement::PINNED_NODE), sinkPlacement);

        for (auto& source : query->getOperatorByType<SourceDescriptorLogicalOperator>())
        {
            auto [_, sourcePlacement] = sourceCatalog.at(source->getSourceDescriptorRef().logicalSourceName);
            source->addProperty(std::string(Distributed::BottomUpPlacement::PINNED_NODE), sourcePlacement);
        }

        return query;
    }

    std::unordered_map<std::string, std::pair<std::string, Distributed::Topology::Node>> sourceCatalog;
    std::unordered_map<std::string, Distributed::Topology::Node> sinkCatalog;
};

TEST_F(PlacementTest, BasicPlacementSingleNode)
{
    Distributed::BottomUpPlacement strategy{};

    Distributed::Topology topology;
    const auto sinkNode = topology.addNode();

    sourceCatalog.try_emplace("Source", "Source", sinkNode);
    sinkCatalog.try_emplace("Sink", sinkNode);

    strategy.capacity[sinkNode] = 10;

    auto plan = getQuery("SELECT * FROM Source WHERE a > b  INTO Sink");

    strategy.doPlacement(topology, *plan);
    auto dag = std::ranges::to<std::vector>(decompose(topology, *plan));

    EXPECT_EQ(dag[0].nodeId, sinkNode);
    EXPECT_EQ(dag[0].successorNode, INVALID<Distributed::Topology::Node>);
    EXPECT_EQ(dag[0].plan->getOperatorByType<SinkLogicalOperator>().size(), 1);
    EXPECT_EQ(dag[0].plan->getOperatorByType<LogicalSelectionOperator>().size(), 1);
    EXPECT_EQ(dag[0].plan->getOperatorByType<SourceDescriptorLogicalOperator>().size(), 1);
    EXPECT_EQ(dag[0].plan->getAllOperators().size(), 3);
}

TEST_F(PlacementTest, BasicPlacement)
{
    Distributed::BottomUpPlacement strategy{};

    Distributed::Topology topology;
    const auto sinkNode = topology.addNode();
    const auto sourceNode = topology.addNode();
    topology.addUpstreams(sinkNode, sourceNode);

    sourceCatalog.try_emplace("Source", "Source", sourceNode);
    sinkCatalog.try_emplace("Sink", sinkNode);

    strategy.capacity[sinkNode] = 1;
    strategy.capacity[sourceNode] = 0;

    auto plan = getQuery("SELECT * FROM Source WHERE a > b  INTO Sink");

    strategy.doPlacement(topology, *plan);
    auto dag = std::ranges::to<std::vector>(decompose(topology, *plan));

    EXPECT_EQ(dag[0].nodeId, sinkNode);
    EXPECT_EQ(dag[0].successorNode, INVALID<Distributed::Topology::Node>);
    EXPECT_EQ(dag[0].plan->getOperatorByType<SinkLogicalOperator>().size(), 1);
    EXPECT_EQ(dag[0].plan->getOperatorByType<SourceDescriptorLogicalOperator>().size(), 0);
    EXPECT_EQ(dag[0].plan->getAllOperators().size(), 2);

    EXPECT_EQ(dag[1].nodeId, sourceNode);
    EXPECT_EQ(dag[1].successorNode, sinkNode);
    EXPECT_EQ(dag[1].plan->getAllOperators().size(), 1);
    EXPECT_EQ(dag[1].plan->getOperatorByType<SinkLogicalOperator>().size(), 0);
    EXPECT_EQ(dag[1].plan->getOperatorByType<SourceDescriptorLogicalOperator>().size(), 1);
}

TEST_F(PlacementTest, JoinPlacement)
{
    Distributed::BottomUpPlacement strategy{};

    Distributed::Topology topology;
    const auto sinkNode = topology.addNode();
    const auto sourceNode1 = topology.addNode();
    const auto sourceNode2 = topology.addNode();
    topology.addUpstreams(sinkNode, sourceNode1);
    topology.addUpstreams(sinkNode, sourceNode2);

    sourceCatalog.try_emplace("stream", "stream", sourceNode1);
    sourceCatalog.try_emplace("stream2", "stream2", sourceNode2);
    sinkCatalog.try_emplace("sinkStreamStream2", sinkNode);

    strategy.capacity[sinkNode] = 10;
    strategy.capacity[sourceNode1] = 10;
    strategy.capacity[sourceNode2] = 10;

    auto plan = getQuery(
        "SELECT * FROM (SELECT * FROM stream) INNER JOIN (SELECT * FROM stream2 WHERE A < B) ON id = id2 WINDOW TUMBLING (timestamp, size "
        "1 sec) INTO "
        "sinkStreamStream2;");

    strategy.doPlacement(topology, *plan);
    auto dag = std::ranges::to<std::vector>(decompose(topology, *plan));

    EXPECT_EQ(dag[0].nodeId, sinkNode);
    EXPECT_EQ(dag[0].successorNode, INVALID<Distributed::Topology::Node>);
    EXPECT_EQ(dag[0].plan->getOperatorByType<SinkLogicalOperator>().size(), 1);
    EXPECT_EQ(dag[0].plan->getOperatorByType<SourceDescriptorLogicalOperator>().size(), 0);
    EXPECT_EQ(dag[0].plan->getAllOperators().size(), 2);

    EXPECT_EQ(dag[1].nodeId, sourceNode1);
    EXPECT_EQ(dag[1].successorNode, sinkNode);
    EXPECT_EQ(dag[1].plan->getAllOperators().size(), 2);
    EXPECT_EQ(dag[1].plan->getOperatorByType<SinkLogicalOperator>().size(), 0);
    EXPECT_EQ(dag[1].plan->getOperatorByType<SourceDescriptorLogicalOperator>().size(), 1);
    EXPECT_EQ(dag[1].plan->getOperatorByType<WatermarkAssignerLogicalOperator>().size(), 1);

    EXPECT_EQ(dag[2].nodeId, sourceNode2);
    EXPECT_EQ(dag[2].successorNode, sinkNode);
    EXPECT_EQ(dag[2].plan->getAllOperators().size(), 3);
    EXPECT_EQ(dag[2].plan->getOperatorByType<SinkLogicalOperator>().size(), 0);
    EXPECT_EQ(dag[2].plan->getOperatorByType<SourceDescriptorLogicalOperator>().size(), 1);
    EXPECT_EQ(dag[2].plan->getOperatorByType<WatermarkAssignerLogicalOperator>().size(), 1);
    EXPECT_EQ(dag[2].plan->getOperatorByType<LogicalSelectionOperator>().size(), 1);
}

TEST_F(PlacementTest, JoinPlacementWithLimitedCapacity)
{
    Distributed::BottomUpPlacement strategy{};

    Distributed::Topology topology;
    const auto sinkNode = topology.addNode();
    const auto sourceNode1 = topology.addNode();
    const auto sourceNode2 = topology.addNode();
    topology.addUpstreams(sinkNode, sourceNode1);
    topology.addUpstreams(sinkNode, sourceNode2);

    sourceCatalog.try_emplace("stream", "stream", sourceNode1);
    sourceCatalog.try_emplace("stream2", "stream2", sourceNode2);
    sinkCatalog.try_emplace("sinkStreamStream2", sinkNode);

    strategy.capacity[sinkNode] = 10;
    strategy.capacity[sourceNode1] = 10;
    strategy.capacity[sourceNode2] = 1;

    auto plan = getQuery(
        "SELECT * FROM (SELECT * FROM stream) INNER JOIN (SELECT * FROM stream2 WHERE A < B) ON id = id2 WINDOW TUMBLING (timestamp, size "
        "1 sec) INTO "
        "sinkStreamStream2;");

    strategy.doPlacement(topology, *plan);
    auto dag = std::ranges::to<std::vector>(decompose(topology, *plan));

    EXPECT_EQ(dag[0].nodeId, sinkNode);
    EXPECT_EQ(dag[0].successorNode, INVALID<Distributed::Topology::Node>);
    EXPECT_EQ(dag[0].plan->getOperatorByType<SinkLogicalOperator>().size(), 1);
    EXPECT_EQ(dag[0].plan->getOperatorByType<SourceDescriptorLogicalOperator>().size(), 0);
    EXPECT_EQ(dag[0].plan->getOperatorByType<WatermarkAssignerLogicalOperator>().size(), 1);
    EXPECT_EQ(dag[0].plan->getAllOperators().size(), 3);

    EXPECT_EQ(dag[1].nodeId, sourceNode1);
    EXPECT_EQ(dag[1].successorNode, sinkNode);
    EXPECT_EQ(dag[1].plan->getAllOperators().size(), 2);
    EXPECT_EQ(dag[1].plan->getOperatorByType<SinkLogicalOperator>().size(), 0);
    EXPECT_EQ(dag[1].plan->getOperatorByType<SourceDescriptorLogicalOperator>().size(), 1);
    EXPECT_EQ(dag[1].plan->getOperatorByType<WatermarkAssignerLogicalOperator>().size(), 1);

    EXPECT_EQ(dag[2].nodeId, sourceNode2);
    EXPECT_EQ(dag[2].successorNode, sinkNode);
    EXPECT_EQ(dag[2].plan->getAllOperators().size(), 2);
    EXPECT_EQ(dag[2].plan->getOperatorByType<SinkLogicalOperator>().size(), 0);
    EXPECT_EQ(dag[2].plan->getOperatorByType<SourceDescriptorLogicalOperator>().size(), 1);
    EXPECT_EQ(dag[2].plan->getOperatorByType<LogicalSelectionOperator>().size(), 1);
}

TEST_F(PlacementTest, JoinPlacementWithLimitedCapacityOnTwoNodes)
{
    Distributed::BottomUpPlacement strategy{};

    Distributed::Topology topology;
    const auto sinkNode = topology.addNode();
    const auto sourceNode1 = topology.addNode();
    topology.addUpstreams(sinkNode, sourceNode1);

    sourceCatalog.try_emplace("stream", "stream", sourceNode1);
    sourceCatalog.try_emplace("stream2", "stream2", sourceNode1);
    sinkCatalog.try_emplace("sinkStreamStream2", sinkNode);

    strategy.capacity[sinkNode] = 10;
    strategy.capacity[sourceNode1] = 3;

    auto plan = getQuery(
        "SELECT * FROM (SELECT * FROM stream) INNER JOIN (SELECT * FROM stream2 WHERE A < B) ON id = id2 WINDOW TUMBLING (timestamp, size "
        "1 sec) INTO "
        "sinkStreamStream2;");

    strategy.doPlacement(topology, *plan);
    auto dag = std::ranges::to<std::vector>(decompose(topology, *plan));

    for (const auto& [plan, node, successorNode] : dag)
    {
        NES_INFO("Node: {}. Successor: {}, Plan:\n{}", node, successorNode, plan->toString());
    }

    EXPECT_EQ(dag[0].nodeId, sinkNode);
    EXPECT_EQ(dag[0].successorNode, INVALID<Distributed::Topology::Node>);
    EXPECT_EQ(dag[0].plan->getAllOperators().size(), 2);
    EXPECT_EQ(dag[0].plan->getOperatorByType<SinkLogicalOperator>().size(), 1);
    EXPECT_EQ(dag[0].plan->getOperatorByType<SourceDescriptorLogicalOperator>().size(), 0);
    EXPECT_EQ(dag[0].plan->getOperatorByType<WatermarkAssignerLogicalOperator>().size(), 0);

    EXPECT_EQ(dag[1].nodeId, sourceNode1);
    EXPECT_EQ(dag[1].successorNode, sinkNode);
    EXPECT_EQ(dag[1].plan->getAllOperators().size(), 5);
    EXPECT_EQ(dag[1].plan->getOperatorByType<SinkLogicalOperator>().size(), 0);
    EXPECT_EQ(dag[1].plan->getOperatorByType<SourceDescriptorLogicalOperator>().size(), 2);
    EXPECT_EQ(dag[1].plan->getOperatorByType<WatermarkAssignerLogicalOperator>().size(), 2);
    EXPECT_EQ(dag[1].plan->getOperatorByType<LogicalSelectionOperator>().size(), 1);
}

TEST_F(PlacementTest, JoinPlacementWithLimitedCapacityOnTwoNodesDecompose)
{
    Distributed::BottomUpPlacement strategy{};

    Distributed::Topology topology;
    const auto sinkNode = topology.addNode();
    const auto sourceNode1 = topology.addNode();
    topology.addUpstreams(sinkNode, sourceNode1);

    sourceCatalog.try_emplace("stream", "stream", sourceNode1);
    sourceCatalog.try_emplace("stream2", "stream2", sourceNode1);
    sinkCatalog.try_emplace("sinkStreamStream2", sinkNode);

    strategy.capacity[sinkNode] = 10;
    strategy.capacity[sourceNode1] = 3;

    auto plan = getQuery(
        "SELECT * FROM (SELECT * FROM stream) INNER JOIN (SELECT * FROM stream2 WHERE A < B) ON id = id2 WINDOW TUMBLING (timestamp, size "
        "1 sec) INTO "
        "sinkStreamStream2;");

    strategy.doPlacement(topology, *plan);
    auto decomposed = decompose(topology, *plan);
    auto plans = Distributed::connect(
        decomposed,
        {{sinkNode, Distributed::PhysicalNodeConfig{.connection = "23", .grpc = ""}},
         {sourceNode1, Distributed::PhysicalNodeConfig{.connection = "24", .grpc = ""}}});

    for (const auto& plan : plans)
    {
        NES_INFO("\n{}", plan->toString());
    }
}

std::shared_ptr<DecomposedQueryPlan> getPlanForNode(std::string grpc, const std::vector<std::shared_ptr<DecomposedQueryPlan>>& plans)
{
    auto it = std::ranges::find(plans, grpc, [](const auto& sharedQp) { return sharedQp->getGRPC(); });
    if (it == plans.end())
    {
        throw TestException("Query plan was not generated for {}", grpc);
    }
    return *it;
}

TEST_F(PlacementTest, BridgePlacment)
{
    Distributed::BottomUpPlacement strategy{};

    Distributed::Topology topology;
    const auto sinkNode = topology.addNode();
    const auto middleNode = topology.addNode();
    const auto sourceNode = topology.addNode();
    topology.addUpstreams(sinkNode, middleNode);
    topology.addUpstreams(middleNode, sourceNode);

    sourceCatalog.try_emplace("source", "source", sourceNode);
    sinkCatalog.try_emplace("sink", sinkNode);

    strategy.capacity[sinkNode] = 10;
    strategy.capacity[sourceNode] = 10;

    auto plan = getQuery("SELECT * FROM source INTO sink");

    strategy.doPlacement(topology, *plan);
    auto decomposed = decompose(topology, *plan);
    auto plans = Distributed::connect(
        decomposed,
        {{sinkNode, Distributed::PhysicalNodeConfig{.connection = "23", .grpc = "sink"}},
         {middleNode, Distributed::PhysicalNodeConfig{.connection = "24", .grpc = "middle"}},
         {sourceNode, Distributed::PhysicalNodeConfig{.connection = "25", .grpc = "source"}}});

    auto middlePlan = getPlanForNode("middle", plans);
    ASSERT_THAT(middlePlan->getAllOperators(), ::testing::SizeIs(2));
    ASSERT_THAT(middlePlan->getSourceOperators<SourceDescriptorLogicalOperator>(), ::testing::SizeIs(1));
    EXPECT_THAT(middlePlan->getSourceOperators<SourceDescriptorLogicalOperator>().at(0)->getSourceDescriptorRef().sourceType, "Network");

    auto sourcePlan = getPlanForNode("source", plans);
    ASSERT_THAT(sourcePlan->getAllOperators(), ::testing::SizeIs(2));
    ASSERT_THAT(sourcePlan->getSourceOperators<SourceDescriptorLogicalOperator>(), ::testing::SizeIs(1));
    EXPECT_THAT(sourcePlan->getSourceOperators<SourceDescriptorLogicalOperator>().at(0)->getSourceDescriptorRef().sourceType, "TEST");
}


TEST_F(PlacementTest, BridgePlacmentJoin)
{
    Distributed::BottomUpPlacement strategy{};

    Distributed::Topology topology;
    const auto sinkNode = topology.addNode();
    const auto middleNode = topology.addNode();
    const auto sourceNode = topology.addNode();
    topology.addUpstreams(sinkNode, middleNode);
    topology.addUpstreams(middleNode, sourceNode);

    sourceCatalog.try_emplace("stream", "stream", sourceNode);
    sourceCatalog.try_emplace("stream2", "stream2", sourceNode);
    sinkCatalog.try_emplace("sinkStreamStream2", sinkNode);

    strategy.capacity[sinkNode] = 10;
    strategy.capacity[sourceNode] = 3;

    auto plan = getQuery(
        "SELECT * FROM (SELECT * FROM stream) INNER JOIN (SELECT * FROM stream2 WHERE A < B) ON id = id2 WINDOW TUMBLING (timestamp, size "
        "1 sec) INTO "
        "sinkStreamStream2;");

    strategy.doPlacement(topology, *plan);
    auto decomposed = decompose(topology, *plan);
    auto plans = Distributed::connect(
        decomposed,
        {{sinkNode, Distributed::PhysicalNodeConfig{.connection = "23", .grpc = "sink"}},
         {middleNode, Distributed::PhysicalNodeConfig{.connection = "24", .grpc = "middle"}},
         {sourceNode, Distributed::PhysicalNodeConfig{.connection = "25", .grpc = "source"}}});

    auto middlePlan = getPlanForNode("middle", plans);
    ASSERT_THAT(middlePlan->getAllOperators(), ::testing::SizeIs(4)) << "Expected two Sink and two Sources";
    ASSERT_THAT(middlePlan->getSourceOperators<SourceDescriptorLogicalOperator>(), ::testing::SizeIs(2));
    EXPECT_THAT(middlePlan->getSourceOperators<SourceDescriptorLogicalOperator>().at(0)->getSourceDescriptorRef().sourceType, "Network");
    EXPECT_THAT(middlePlan->getSourceOperators<SourceDescriptorLogicalOperator>().at(1)->getSourceDescriptorRef().sourceType, "Network");

    for (const auto& plan : plans)
    {
        NES_INFO("{}\n{}", plan->getGRPC(), plan->toString());
    }

    auto sourcePlan = getPlanForNode("source", plans);
    ASSERT_THAT(sourcePlan->getAllOperators(), ::testing::SizeIs(7)) << "Expected 2 Sinks 2 Sources. 2 Watermark assigner and 1 Filter";
    ASSERT_THAT(sourcePlan->getSourceOperators<SourceDescriptorLogicalOperator>(), ::testing::SizeIs(2));
    EXPECT_THAT(sourcePlan->getSourceOperators<SourceDescriptorLogicalOperator>().at(0)->getSourceDescriptorRef().sourceType, "TEST");
    EXPECT_THAT(sourcePlan->getSourceOperators<SourceDescriptorLogicalOperator>().at(1)->getSourceDescriptorRef().sourceType, "TEST");
}

TEST_F(PlacementTest, ComplexQuery)
{
    Distributed::BottomUpPlacement strategy{};

    Distributed::Topology topology;
    const auto sinkNode = topology.addNode();
    const auto middleNode = topology.addNode();
    const auto sourceNode = topology.addNode();
    topology.addUpstreams(sinkNode, middleNode);
    topology.addUpstreams(middleNode, sourceNode);

    sourceCatalog.try_emplace("stream", "stream", sourceNode);
    sourceCatalog.try_emplace("stream2", "stream2", sourceNode);
    sinkCatalog.try_emplace("sinkStreamStream2", sinkNode);

    strategy.capacity[sinkNode] = 10;
    strategy.capacity[sourceNode] = 3;

    auto plan = getQuery("SELECT * FROM (SELECT * FROM stream) INNER JOIN (SELECT * FROM stream2) ON id "
                         "= id2 WINDOW SLIDING (timestamp, size 1 sec, advance by 500 ms) INNER JOIN (SELECT * FROM stream) "
                         "ON id = id4 WINDOW SLIDING (timestamp, size 1 sec, advance by 500 ms) INNER JOIN (SELECT * FROM "
                         "stream2) ON id = id4_1 WINDOW SLIDING (timestamp, size 1 sec, advance by 500 ms) INTO sinkStreamStream2");

    strategy.doPlacement(topology, *plan);
    auto decomposed = decompose(topology, *plan);
    auto plans = Distributed::connect(
        decomposed,
        {{sinkNode, Distributed::PhysicalNodeConfig{.connection = "23", .grpc = "sink"}},
         {middleNode, Distributed::PhysicalNodeConfig{.connection = "24", .grpc = "middle"}},
         {sourceNode, Distributed::PhysicalNodeConfig{.connection = "25", .grpc = "source"}}});

    for (const auto& plan : plans)
    {
        NES_INFO("{}\n{}", plan->getGRPC(), plan->toString());
    }
}

}
