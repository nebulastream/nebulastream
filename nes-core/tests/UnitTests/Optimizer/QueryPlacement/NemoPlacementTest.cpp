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

#include "z3++.h"
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <gtest/gtest.h>
#include <utility>

using namespace NES;
using namespace z3;
using namespace Configurations;

class NemoPlacementTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    z3::ContextPtr z3Context;
    SourceCatalogPtr sourceCatalog;
    TopologyPtr topology;
    QueryParsingServicePtr queryParsingService;
    GlobalExecutionPlanPtr globalExecutionPlan;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup NemoPlacementTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Logger::setupLogging("NemoPlacementTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup NemoPlacementTest test case." << std::endl;
        z3Context = std::make_shared<z3::context>();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Setup NemoPlacementTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down NemoPlacementTest test class." << std::endl; }

    void setupTopologyAndSourceCatalog(uint64_t layers, uint64_t nodesPerNode, uint64_t leafNodesPerNode) {
        uint64_t resources = 100;
        uint64_t nodeId = 1;
        uint64_t leafNodes = 0;

        std::vector<TopologyNodePtr> nodes;
        std::vector<TopologyNodePtr> parents;

        // Setup the topology
        auto rootNode = TopologyNode::create(nodeId, "localhost", 4000, 5000, resources);
        topology = Topology::create();
        topology->setAsRoot(rootNode);
        nodes.emplace_back(rootNode);
        parents.emplace_back(rootNode);

        for (uint64_t i = 2; i <= layers; i++) {
            std::vector<TopologyNodePtr> newParents;
            for (auto parent : parents) {
                uint64_t nodeCnt = nodesPerNode;
                if (i == layers) {
                    nodeCnt = leafNodesPerNode;
                }
                for (uint64_t j = 0; j < nodeCnt; j++) {
                    if (i == layers) {
                        leafNodes++;
                    }
                    nodeId++;
                    auto newNode = TopologyNode::create(nodeId, "localhost", 4000 + nodeId, 5000 + nodeId, resources);
                    topology->addNewTopologyNodeAsChild(parent, newNode);
                    nodes.emplace_back(newNode);
                    newParents.emplace_back(newNode);
                }
            }
            parents = newParents;
        }

        NES_DEBUG("NemoPlacementTest: topology: " << topology->toString());

        // Prepare the source and schema
        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64)"
                             "->addField(\"timestamp\", DataTypeFactory::createUInt64());";
        const std::string sourceName = "car";

        sourceCatalog = std::make_shared<SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);

        uint64_t childIndex = nodes.size() - leafNodes;
        for (uint16_t i = childIndex; i < nodes.size(); i++) {
            CSVSourceTypePtr csvSourceType = CSVSourceType::create();
            csvSourceType->setGatheringInterval(0);
            csvSourceType->setNumberOfTuplesToProducePerBuffer(0);

            auto physicalSource = PhysicalSource::create(sourceName, "test" + std::to_string(i), csvSourceType);
            SourceCatalogEntryPtr sourceCatalogEntry1 =
                std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, nodes[i]);
            sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
        }
    }

    static void assignDataModificationFactor(QueryPlanPtr queryPlan) {
        QueryPlanIterator queryPlanIterator = QueryPlanIterator(std::move(queryPlan));

        for (auto qPlanItr = queryPlanIterator.begin(); qPlanItr != QueryPlanIterator::end(); ++qPlanItr) {
            // set data modification factor for map operator
            if ((*qPlanItr)->instanceOf<MapLogicalOperatorNode>()) {
                auto op = (*qPlanItr)->as<MapLogicalOperatorNode>();
                NES_DEBUG("input schema in bytes: " << op->getInputSchema()->getSchemaSizeInBytes());
                NES_DEBUG("output schema in bytes: " << op->getOutputSchema()->getSchemaSizeInBytes());
                double schemaSizeComparison =
                    1.0 * op->getOutputSchema()->getSchemaSizeInBytes() / op->getInputSchema()->getSchemaSizeInBytes();

                op->addProperty("DMF", schemaSizeComparison);
            }
        }
    }
};

/* Test query placement with bottom up strategy  */
TEST_F(NemoPlacementTest, testNemoPlacementFlatTopology) {
    setupTopologyAndSourceCatalog(2, 10, 10);

    Query query = Query::from("car")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .apply(Count()->as(Attribute("count_value")))
                      .sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto optimizerConfig = Configurations::OptimizerConfiguration();
    optimizerConfig.distributedWindowChildThreshold = 0;
    optimizerConfig.distributedWindowCombinerThreshold = 1000;
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, optimizerConfig);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
    NES_DEBUG("NemoPlacementTest: topology: " << topology->toString());
    NES_DEBUG("NemoPlacementTest: query plan " << globalExecutionPlan->getAsString());
    NES_DEBUG("NemoPlacementTest: shared plan \n" << sharedQueryPlan->getQueryPlan()->toString());

    //EXPECT_EQ(executionNodes.size(), 3UL);
}

TEST_F(NemoPlacementTest, testNemoPlacementThreeLevelsTopology) {
    setupTopologyAndSourceCatalog(3, 10, 10);

    Query query = Query::from("car")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .apply(Count()->as(Attribute("count_value")))
                      .sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto optimizerConfig = Configurations::OptimizerConfiguration();
    optimizerConfig.distributedWindowChildThreshold = 1;
    optimizerConfig.distributedWindowCombinerThreshold = 1;
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, optimizerConfig);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
    NES_DEBUG("NemoPlacementTest: topology: " << topology->toString());
    NES_DEBUG("NemoPlacementTest: query plan " << globalExecutionPlan->getAsString());
    NES_DEBUG("NemoPlacementTest: shared plan \n" << sharedQueryPlan->getQueryPlan()->toString());

    //EXPECT_EQ(executionNodes.size(), 3UL);
}

TEST_F(NemoPlacementTest, testNemoPlacementFourLevelsSparseTopology) {
    setupTopologyAndSourceCatalog(4, 2, 1);

    Query query = Query::from("car")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .apply(Count()->as(Attribute("count_value")))
                      .sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto optimizerConfig = Configurations::OptimizerConfiguration();
    optimizerConfig.distributedWindowChildThreshold = 0;
    optimizerConfig.distributedWindowCombinerThreshold = 1;
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, optimizerConfig);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
    NES_DEBUG("NemoPlacementTest: topology: " << topology->toString());
    NES_DEBUG("NemoPlacementTest: query plan " << globalExecutionPlan->getAsString());
    NES_DEBUG("NemoPlacementTest: shared plan \n" << sharedQueryPlan->getQueryPlan()->toString());

    //EXPECT_EQ(executionNodes.size(), 3UL);
}