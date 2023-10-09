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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
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
#include <Util/Experimental/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <gtest/gtest.h>
#include <utility>
#include <z3++.h>

using namespace NES;
using namespace z3;
using namespace Configurations;

class NemoPlacementTest : public Testing::BaseUnitTest {
  public:
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    QueryPlanPtr queryPlan;
    SharedQueryPlanPtr sharedQueryPlan;
    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    QueryParsingServicePtr queryParsingService;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NemoPlacementTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup NemoPlacementTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_DEBUG("Setup NemoPlacementTest test case.");
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
    }

    void setupTopologyAndSourceCatalog(uint64_t layers, uint64_t nodesPerNode, uint64_t leafNodesPerNode) {
        uint64_t resources = 100;
        uint64_t nodeId = 1;
        uint64_t leafNodes = 0;

        std::vector<TopologyNodePtr> nodes;
        std::vector<TopologyNodePtr> parents;

        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

        // Setup the topology
        auto rootNode = TopologyNode::create(nodeId, "localhost", 4000, 5000, resources, properties);
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
                    auto newNode = TopologyNode::create(nodeId, "localhost", 4000 + nodeId, 5000 + nodeId, resources, properties);
                    topology->addNewTopologyNodeAsChild(parent, newNode);
                    nodes.emplace_back(newNode);
                    newParents.emplace_back(newNode);
                }
            }
            parents = newParents;
        }

        NES_DEBUG("NemoPlacementTest: topology: {}", topology->toString());

        // Prepare the source and schema
        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64)"
                             "->addField(\"timestamp\", DataTypeFactory::createUInt64());";
        const std::string sourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);

        uint64_t childIndex = nodes.size() - leafNodes;
        for (uint16_t i = childIndex; i < nodes.size(); i++) {
            CSVSourceTypePtr csvSourceType = CSVSourceType::create();
            csvSourceType->setGatheringInterval(0);
            csvSourceType->setNumberOfTuplesToProducePerBuffer(0);

            auto physicalSource = PhysicalSource::create(sourceName, "test" + std::to_string(i), csvSourceType);
            Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
                std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, nodes[i]);
            sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
        }
    }

    void runNemoPlacement(OptimizerConfiguration optimizerConfig) {
        Query query = Query::from("car")
                          .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                          .apply(Count()->as(Attribute("count_value")))
                          .sink(NullOutputSinkDescriptor::create());
        queryPlan = query.getQueryPlan();
        queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);

        // Prepare the placement
        globalExecutionPlan = GlobalExecutionPlan::create();
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

        // Execute optimization phases prior to placement
        queryPlan = typeInferencePhase->execute(queryPlan);

        auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
        auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
        queryPlan = queryReWritePhase->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);

        auto topologySpecificQueryRewrite =
            Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, optimizerConfig);
        topologySpecificQueryRewrite->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);

        assignDataModificationFactor(queryPlan);

        // Execute the placement
        sharedQueryPlan = SharedQueryPlan::create(queryPlan);
        auto queryPlacementPhase =
            Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
        queryPlacementPhase->execute(sharedQueryPlan);
    }

    static void assignDataModificationFactor(QueryPlanPtr queryPlan) {
        QueryPlanIterator queryPlanIterator = QueryPlanIterator(std::move(queryPlan));

        for (auto qPlanItr = queryPlanIterator.begin(); qPlanItr != QueryPlanIterator::end(); ++qPlanItr) {
            // set data modification factor for map operator
            if ((*qPlanItr)->instanceOf<MapLogicalOperatorNode>()) {
                auto op = (*qPlanItr)->as<MapLogicalOperatorNode>();
                NES_DEBUG("input schema in bytes: {}", op->getInputSchema()->getSchemaSizeInBytes());
                NES_DEBUG("output schema in bytes: {}", op->getOutputSchema()->getSchemaSizeInBytes());
                double schemaSizeComparison =
                    1.0 * op->getOutputSchema()->getSchemaSizeInBytes() / op->getInputSchema()->getSchemaSizeInBytes();

                op->addProperty("DMF", schemaSizeComparison);
            }
        }
    }

    template<typename T>
    static void verifyChildrenOfType(std::vector<QueryPlanPtr>& querySubPlans, uint64_t expectedSubPlanSize = 1) {
        ASSERT_EQ(querySubPlans.size(), expectedSubPlanSize);
        for (auto& querySubPlan : querySubPlans) {
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            auto children = actualRootOperator->getChildren();
            for (const auto& child : children) {
                ASSERT_TRUE(child->instanceOf<T>());
            }
        }
    }

    template<typename T>
    static void verifySourceOperators(std::vector<QueryPlanPtr>& querySubPlans,
                                      uint64_t expectedSubPlanSize,
                                      uint64_t expectedSourceOperatorSize) {
        ASSERT_EQ(querySubPlans.size(), expectedSubPlanSize);
        auto querySubPlan = querySubPlans[0];
        std::vector<SourceLogicalOperatorNodePtr> sourceOperators = querySubPlan->getSourceOperators();
        ASSERT_EQ(sourceOperators.size(), expectedSourceOperatorSize);
        for (const auto& sourceOperator : sourceOperators) {
            EXPECT_TRUE(sourceOperator->instanceOf<SourceLogicalOperatorNode>());
            auto sourceDescriptor = sourceOperator->as_if<SourceLogicalOperatorNode>()->getSourceDescriptor();
            ASSERT_TRUE(sourceDescriptor->instanceOf<T>());
        }
    }
};

/* Test query placement with bottom up strategy  */
TEST_F(NemoPlacementTest, testNemoPlacementFlatTopologyNoMerge) {
    setupTopologyAndSourceCatalog(2, 10, 10);
    auto optimizerConfig = Configurations::OptimizerConfiguration();
    optimizerConfig.enableNemoPlacement = true;
    optimizerConfig.distributedWindowChildThreshold = 0;
    optimizerConfig.distributedWindowCombinerThreshold = 1000;

    //run the placement
    runNemoPlacement(optimizerConfig);

    auto sharedQueryId = sharedQueryPlan->getId();
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
    NES_DEBUG("NemoPlacementTest: topology: {}\n", topology->toString());
    NES_DEBUG("NemoPlacementTest: query plan {}\n", globalExecutionPlan->getAsString());
    NES_DEBUG("NemoPlacementTest: shared plan {}\n", sharedQueryPlan->getQueryPlan()->toString());

    //Assertion
    ASSERT_EQ(executionNodes.size(), 11u);
    for (const auto& executionNode : executionNodes) {
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryId);
        if (executionNode->getId() == 1u) {
            verifyChildrenOfType<SourceLogicalOperatorNode>(querySubPlans);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 1, 10);
        } else {
            verifyChildrenOfType<CentralWindowOperator>(querySubPlans);
            verifySourceOperators<LogicalSourceDescriptor>(querySubPlans, 1, 1);
        }
    }
}

/* Test query placement with bottom up strategy  */
TEST_F(NemoPlacementTest, testNemoPlacementFlatTopologyMerge) {
    setupTopologyAndSourceCatalog(2, 10, 10);
    auto optimizerConfig = Configurations::OptimizerConfiguration();
    optimizerConfig.enableNemoPlacement = true;
    optimizerConfig.distributedWindowChildThreshold = 0;
    optimizerConfig.distributedWindowCombinerThreshold = 0;

    //run the placement
    runNemoPlacement(optimizerConfig);

    auto sharedQueryId = sharedQueryPlan->getId();
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
    NES_DEBUG("NemoPlacementTest: topology: {}\n", topology->toString());
    NES_DEBUG("NemoPlacementTest: query plan {}\n", globalExecutionPlan->getAsString());
    NES_DEBUG("NemoPlacementTest: shared plan {}\n", sharedQueryPlan->getQueryPlan()->toString());

    //Assertion
    ASSERT_EQ(executionNodes.size(), 11u);
    for (const auto& executionNode : executionNodes) {
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryId);
        if (executionNode->getId() == 1u) {
            verifyChildrenOfType<CentralWindowOperator>(querySubPlans);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 1, 10);
        } else {
            verifyChildrenOfType<WatermarkAssignerLogicalOperatorNode>(querySubPlans);
            verifySourceOperators<LogicalSourceDescriptor>(querySubPlans, 1, 1);
        }
    }
}

TEST_F(NemoPlacementTest, testNemoPlacementThreeLevelsTopology) {
    setupTopologyAndSourceCatalog(3, 10, 10);

    auto optimizerConfig = Configurations::OptimizerConfiguration();
    optimizerConfig.enableNemoPlacement = true;
    optimizerConfig.distributedWindowChildThreshold = 0;
    optimizerConfig.distributedWindowCombinerThreshold = 0;

    //run the placement
    runNemoPlacement(optimizerConfig);

    auto sharedQueryId = sharedQueryPlan->getId();
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
    NES_DEBUG("NemoPlacementTest: topology: {}\n", topology->toString());
    NES_DEBUG("NemoPlacementTest: query plan {}\n", globalExecutionPlan->getAsString());
    NES_DEBUG("NemoPlacementTest: shared plan {}\n", sharedQueryPlan->getQueryPlan()->toString());

    //Assertion
    ASSERT_EQ(executionNodes.size(), 111u);
    for (const auto& executionNode : executionNodes) {
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryId);
        if (executionNode->getId() == 1u) {
            //coordinator
            NES_DEBUG("NemoPlacementTest: Testing Coordinator");
            verifyChildrenOfType<SourceLogicalOperatorNode>(querySubPlans, 1);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 1, 10);
        } else if (executionNode->getId() >= 2u && executionNode->getId() <= 11u) {
            //intermediate nodes
            NES_DEBUG("NemoPlacementTest: Testing 1st level of the tree");
            verifyChildrenOfType<CentralWindowOperator>(querySubPlans);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 1, 10);
        } else {
            //leaves
            NES_DEBUG("NemoPlacementTest: Testing leaves of the tree");
            verifyChildrenOfType<WatermarkAssignerLogicalOperatorNode>(querySubPlans);
            verifySourceOperators<LogicalSourceDescriptor>(querySubPlans, 1, 1);
        }
    }
}

TEST_F(NemoPlacementTest, testNemoPlacementFourLevelsSparseTopology) {
    setupTopologyAndSourceCatalog(4, 2, 1);

    auto optimizerConfig = Configurations::OptimizerConfiguration();
    optimizerConfig.enableNemoPlacement = true;
    optimizerConfig.distributedWindowChildThreshold = 0;
    optimizerConfig.distributedWindowCombinerThreshold = 0;

    //run the placement
    runNemoPlacement(optimizerConfig);

    auto sharedQueryId = sharedQueryPlan->getId();
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
    NES_DEBUG("NemoPlacementTest: topology: {}\n", topology->toString());
    NES_DEBUG("NemoPlacementTest: query plan {}\n", globalExecutionPlan->getAsString());
    NES_DEBUG("NemoPlacementTest: shared plan {}\n", sharedQueryPlan->getQueryPlan()->toString());

    //Assertion
    ASSERT_EQ(executionNodes.size(), 11u);
    for (const auto& executionNode : executionNodes) {
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryId);
        if (executionNode->getId() == 1u) {
            //coordinator
            NES_DEBUG("NemoPlacementTest: Testing Coordinator");
            verifyChildrenOfType<CentralWindowOperator>(querySubPlans);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 1, 4);
        } else if (executionNode->getId() >= 2u && executionNode->getId() <= 3u) {
            //intermediate nodes
            NES_DEBUG("NemoPlacementTest: Testing 1st level of the tree");
            verifyChildrenOfType<SourceLogicalOperatorNode>(querySubPlans, 2);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 2, 1);
        } else if (executionNode->getId() >= 4u && executionNode->getId() <= 7u) {
            //intermediate nodes
            NES_DEBUG("NemoPlacementTest: Testing 2nd level of the tree");
            verifyChildrenOfType<SourceLogicalOperatorNode>(querySubPlans);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 1, 1);
        } else {
            //leaves
            NES_DEBUG("NemoPlacementTest: Testing leaves of the tree");
            verifyChildrenOfType<WatermarkAssignerLogicalOperatorNode>(querySubPlans);
            verifySourceOperators<LogicalSourceDescriptor>(querySubPlans, 1, 1);
        }
    }
}

TEST_F(NemoPlacementTest, testNemoPlacementFourLevelsDenseTopology) {
    setupTopologyAndSourceCatalog(4, 3, 3);

    auto optimizerConfig = Configurations::OptimizerConfiguration();
    optimizerConfig.enableNemoPlacement = true;
    optimizerConfig.distributedWindowChildThreshold = 0;
    optimizerConfig.distributedWindowCombinerThreshold = 0;

    //run the placement
    runNemoPlacement(optimizerConfig);

    auto sharedQueryId = sharedQueryPlan->getId();
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
    NES_DEBUG("NemoPlacementTest: topology: {}\n", topology->toString());
    NES_DEBUG("NemoPlacementTest: query plan {}\n", globalExecutionPlan->getAsString());
    NES_DEBUG("NemoPlacementTest: shared plan {}\n", sharedQueryPlan->getQueryPlan()->toString());

    //Assertion
    ASSERT_EQ(executionNodes.size(), 40u);
    for (const auto& executionNode : executionNodes) {
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryId);
        if (executionNode->getId() == 1u) {
            //coordinator
            NES_DEBUG("NemoPlacementTest: Testing Coordinator");
            verifyChildrenOfType<SourceLogicalOperatorNode>(querySubPlans);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 1, 9);
        } else if (executionNode->getId() >= 2u && executionNode->getId() <= 4u) {
            //intermediate nodes
            NES_DEBUG("NemoPlacementTest: Testing 1st level of the tree");
            verifyChildrenOfType<SourceLogicalOperatorNode>(querySubPlans, 3);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 3, 1);
        } else if (executionNode->getId() >= 5u && executionNode->getId() <= 13u) {
            //intermediate nodes
            NES_DEBUG("NemoPlacementTest: Testing 2nd level of the tree");
            verifyChildrenOfType<CentralWindowOperator>(querySubPlans);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 1, 3);
        } else {
            //leaves
            NES_DEBUG("NemoPlacementTest: Testing leaves of the tree");
            verifyChildrenOfType<WatermarkAssignerLogicalOperatorNode>(querySubPlans);
            verifySourceOperators<LogicalSourceDescriptor>(querySubPlans, 1, 1);
        }
    }
}