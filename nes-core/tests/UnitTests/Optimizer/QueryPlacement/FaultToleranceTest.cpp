//
// Created by noah on 22.09.22.
//
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

#include "Optimizer/QueryMerger/SyntaxBasedCompleteQueryMergerRule.hpp"
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
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/ILPStrategy.hpp>
#include <Optimizer/QueryPlacement/ManualPlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <utility>

using namespace NES;
        using namespace z3;
        using namespace Configurations;

        class QueryPlacementTest : public Testing::TestWithErrorHandling<testing::Test> {
          public:
            z3::ContextPtr z3Context;
            SourceCatalogPtr sourceCatalog;
            TopologyPtr topology;
            QueryParsingServicePtr queryParsingService;
            GlobalExecutionPlanPtr globalExecutionPlan;
            Optimizer::TypeInferencePhasePtr typeInferencePhase;
            std::shared_ptr<Catalogs::UdfCatalog> udfCatalog;
            std::vector<ExecutionNodePtr> executionNodes;
            QueryId queryId;
            QueryId queryId2;
            /* Will be called before any test in this class are executed. */
            static void SetUpTestCase() { std::cout << "Setup QueryPlacementTest test class." << std::endl; }

            /* Will be called before a test is executed. */
            void SetUp() override {
                NES::Logger::setupLogging("QueryPlacementTest.log", NES::LogLevel::LOG_DEBUG);
                std::cout << "Setup QueryPlacementTest test case." << std::endl;
                z3Context = std::make_shared<z3::context>();
                auto cppCompiler = Compiler::CPPCompiler::create();
                auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
                queryParsingService = QueryParsingService::create(jitCompiler);
                udfCatalog = Catalogs::UdfCatalog::create();
            }

            /* Will be called before a test is executed. */
            void TearDown() override { std::cout << "Setup QueryPlacementTest test case." << std::endl; }

            /* Will be called after all tests in this class are finished. */
            static void TearDownTestCase() { std::cout << "Tear down QueryPlacementTest test class." << std::endl; }


            void setupSimpleQuery(){
                Query query = Query::from("car").filter(Attribute("id") < 45)
                                  .filter(Attribute("id") < 40)
                                  .project(Attribute("id").as("id_new"), Attribute("value"))
                                  .map(Attribute("value") = Attribute("value") + 1)
                                  .as("carRename")
                                  .sink(PrintSinkDescriptor::create());
                QueryPlanPtr queryPlan = query.getQueryPlan();
                Query subQuery1 = Query::from("car");


                auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
                queryPlan = queryReWritePhase->execute(queryPlan);
                typeInferencePhase->execute(queryPlan);

                auto topologySpecificQueryRewrite =
                    Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
                topologySpecificQueryRewrite->execute(queryPlan);
                typeInferencePhase->execute(queryPlan);

                auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
                queryId = sharedQueryPlan->getSharedQueryId();
                auto queryPlacementPhase =
                    Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
                queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);
                executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

                NES_INFO("\n"+globalExecutionPlan->getAsString())
            }

            void setupSimpleTruckQuery(){
                Query query2 = Query::from("truck")
                                   .map(Attribute("value") = 40)
                                   .filter(Attribute("id") < 45)
                                   .sink(PrintSinkDescriptor::create());
                QueryPlanPtr queryPlan2 = query2.getQueryPlan();

                auto queryReWritePhase2 = Optimizer::QueryRewritePhase::create(false);
                queryPlan2 = queryReWritePhase2->execute(queryPlan2);
                typeInferencePhase->execute(queryPlan2);

                auto topologySpecificQueryRewrite2 =
                    Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
                topologySpecificQueryRewrite2->execute(queryPlan2);
                typeInferencePhase->execute(queryPlan2);

                auto sharedQueryPlan2 = SharedQueryPlan::create(queryPlan2);
                queryId2 = sharedQueryPlan2->getSharedQueryId();
                auto queryPlacementPhase2 =
                    Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
                queryPlacementPhase2->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan2);
            }

            void setupSelfJoinQuery(){

                Query query = Query::from("car")
                                  .as("c1")
                                  .joinWith(Query::from("car").as("c2"))
                                  .where(Attribute("id"))
                                  .equalsTo(Attribute("id"))
                                  .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                                  .sink(NullOutputSinkDescriptor::create());
                /*Query query = Query::from("car")
                                  .filter(Attribute("id") < 45)
                                  .filter(Attribute("id") < 45)
                                  .project(Attribute("value"), Attribute("id"))
                                  .sink(PrintSinkDescriptor::create());*/
                QueryPlanPtr queryPlan = query.getQueryPlan();

                auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
                queryPlan = queryReWritePhase->execute(queryPlan);
                typeInferencePhase->execute(queryPlan);

                auto topologySpecificQueryRewrite =
                    Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
                topologySpecificQueryRewrite->execute(queryPlan);
                typeInferencePhase->execute(queryPlan);

                auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
                queryId = sharedQueryPlan->getSharedQueryId();
                auto queryPlacementPhase =
                    Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
                queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);
                executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

                NES_INFO("\n"+globalExecutionPlan->getAsString())
            }

            void setupSmallTopologyAndSourceCatalog(std::vector<uint16_t> resources) {

                topology = Topology::create();

                TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", 123, 124, resources[0]);
                topology->setAsRoot(rootNode);

                TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, resources[1]);
                topology->addNewTopologyNodeAsChild(rootNode, sourceNode1);

                TopologyNodePtr sourceNode2 = TopologyNode::create(3, "localhost", 123, 124, resources[2]);
                topology->addNewTopologyNodeAsChild(rootNode, sourceNode2);

                std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                                     "->addField(\"value\", BasicType::UINT64);";
                const std::string sourceName = "car";

                sourceCatalog = std::make_shared<SourceCatalog>(queryParsingService);
                sourceCatalog->addLogicalSource(sourceName, schema);
                auto logicalSource = sourceCatalog->getLogicalSource(sourceName);

                CSVSourceTypePtr csvSourceType = CSVSourceType::create();
                csvSourceType->setGatheringInterval(0);
                csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
                auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);

                SourceCatalogEntryPtr sourceCatalogEntry1 =
                    std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, sourceNode1);
                SourceCatalogEntryPtr sourceCatalogEntry2 =
                    std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, sourceNode2);

                sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
                sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry2);

                globalExecutionPlan = GlobalExecutionPlan::create();
                typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
            }

            void setupMediumTopologyAndSourceCatalog(std::vector<uint16_t> resources) {

                topology = Topology::create();

                //Test resources
                TopologyNodePtr node1 = TopologyNode::create(1, "localhost", 123, 124, 13);
                topology->setAsRoot(node1);

                //Test resources
                TopologyNodePtr node2 = TopologyNode::create(2, "localhost", 125, 126, 7);
                topology->addNewTopologyNodeAsChild(node1, node2);

                //Test resources
                TopologyNodePtr node3 = TopologyNode::create(3, "localhost", 127, 128, 9);
                topology->addNewTopologyNodeAsChild(node1, node3);

                TopologyNodePtr node4 = TopologyNode::create(12, "localhost", 129, 130, resources[3]);
                topology->addNewTopologyNodeAsChild(node1, node4);

                TopologyNodePtr node5 = TopologyNode::create(13, "localhost", 131, 132, 7);
                topology->addNewTopologyNodeAsChild(node1, node5);

                TopologyNodePtr node6 = TopologyNode::create(14, "localhost", 133, 134, 2);
                topology->addNewTopologyNodeAsChild(node5, node6);

                TopologyNodePtr node7 = TopologyNode::create(15, "localhost", 133, 134, 3);
                topology->addNewTopologyNodeAsChild(node4, node7);

                sourceCatalog = std::make_shared<SourceCatalog>(queryParsingService);

                CSVSourceTypePtr csvSourceType = CSVSourceType::create();
                csvSourceType->setGatheringInterval(0);
                csvSourceType->setNumberOfTuplesToProducePerBuffer(0);

                //SCHEMA 1
                std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                                     "->addField(\"timestamp\", DataTypeFactory::createUInt64())"
                                     "->addField(\"value\", BasicType::UINT64);";
                const std::string sourceName = "car";
                sourceCatalog->addLogicalSource(sourceName, schema);

                auto logicalSource = sourceCatalog->getLogicalSource(sourceName);

                auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);

                SourceCatalogEntryPtr sourceCatalogEntry1 =
                    std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node6);

                sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);

                //SCHEMA 2
                std::string schema2 = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                                      "->addField(\"timestamp\", DataTypeFactory::createUInt64())"
                                      "->addField(\"value\", BasicType::UINT64);";
                const std::string sourceName2 = "truck";
                sourceCatalog->addLogicalSource(sourceName2, schema2);

                auto logicalSource2 = sourceCatalog->getLogicalSource(sourceName2);

                auto physicalSource2 = PhysicalSource::create(sourceName2, "test2", csvSourceType);

                SourceCatalogEntryPtr sourceCatalogEntry2 =
                    std::make_shared<SourceCatalogEntry>(physicalSource2, logicalSource2, node7);


                globalExecutionPlan = GlobalExecutionPlan::create();
                typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
            }

            void setupLargeTopologyAndSourceCatalog(std::vector<uint16_t> resources) {

                topology = Topology::create();

                TopologyNodePtr node1 = TopologyNode::create(1, "localhost", 123, 124, resources[0]);
                topology->setAsRoot(node1);

                TopologyNodePtr node2 = TopologyNode::create(2, "localhost", 125, 126, 4);
                TopologyNodePtr node3 = TopologyNode::create(3, "localhost", 125, 126, 4);
                TopologyNodePtr node4 = TopologyNode::create(4, "localhost", 125, 126, 4);
                TopologyNodePtr node5 = TopologyNode::create(5, "localhost", 125, 126, 4);
                TopologyNodePtr node6 = TopologyNode::create(6, "localhost", 125, 126, 4);
                TopologyNodePtr node7 = TopologyNode::create(7, "localhost", 125, 126, 4);
                TopologyNodePtr node8 = TopologyNode::create(8, "localhost", 125, 126, 4);
                TopologyNodePtr node9 = TopologyNode::create(9, "localhost", 125, 126, 4);
                TopologyNodePtr node10 = TopologyNode::create(10, "localhost", 125, 126, 4);
                TopologyNodePtr node11 = TopologyNode::create(11, "localhost", 125, 126, 4);
                TopologyNodePtr node12 = TopologyNode::create(12, "localhost", 125, 126, 4);
                TopologyNodePtr node13 = TopologyNode::create(13, "localhost", 125, 126, 4);
                TopologyNodePtr node14 = TopologyNode::create(14, "localhost", 125, 126, 4);
                TopologyNodePtr node15 = TopologyNode::create(15, "localhost", 125, 126, 4);
                TopologyNodePtr node16 = TopologyNode::create(16, "localhost", 125, 126, 4);


                topology->addNewTopologyNodeAsChild(node1, node2);
                topology->addNewTopologyNodeAsChild(node1, node3);

                topology->addNewTopologyNodeAsChild(node2, node4);
                topology->addNewTopologyNodeAsChild(node2, node5);

                topology->addNewTopologyNodeAsChild(node3, node6);
                topology->addNewTopologyNodeAsChild(node3, node7);
                topology->addNewTopologyNodeAsChild(node3, node10);

                topology->addNewTopologyNodeAsChild(node4, node8);
                topology->addNewTopologyNodeAsChild(node4, node9);
                topology->addNewTopologyNodeAsChild(node4, node10);

                topology->addNewTopologyNodeAsChild(node5, node9);
                topology->addNewTopologyNodeAsChild(node5, node10);

                topology->addNewTopologyNodeAsChild(node7, node11);
                topology->addNewTopologyNodeAsChild(node7, node12);
                topology->addNewTopologyNodeAsChild(node7, node13);

                topology->addNewTopologyNodeAsChild(node10, node14);
                topology->addNewTopologyNodeAsChild(node10, node15);

                topology->addNewTopologyNodeAsChild(node14, node16);

                topology->addNewTopologyNodeAsChild(node7, node15);



                TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, resources[1]);


                TopologyNodePtr sourceNode2 = TopologyNode::create(3, "localhost", 123, 124, resources[2]);


                std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                                     "->addField(\"value\", BasicType::UINT64)"
                                     "->addField(\"timestamp\", DataTypeFactory::createUInt64());";;
                const std::string sourceName = "car";

                std::string schema2 = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                                      "->addField(\"timestamp\", DataTypeFactory::createUInt64())"
                                      "->addField(\"value\", BasicType::UINT64);";
                const std::string sourceName2 = "truck";

                sourceCatalog = std::make_shared<SourceCatalog>(queryParsingService);
                sourceCatalog->addLogicalSource(sourceName, schema);
                auto logicalSource = sourceCatalog->getLogicalSource(sourceName);

                sourceCatalog->addLogicalSource(sourceName2, schema2);
                auto logicalSource2 = sourceCatalog->getLogicalSource(sourceName2);

                CSVSourceTypePtr csvSourceType = CSVSourceType::create();
                CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
                csvSourceType->setGatheringInterval(0);
                csvSourceType2->setNumberOfTuplesToProducePerBuffer(0);
                csvSourceType2->setGatheringInterval(0);
                csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
                auto physicalSource = PhysicalSource::create(sourceName, "test1", csvSourceType);
                auto physicalSource2 = PhysicalSource::create(sourceName2, "test2", csvSourceType2);

                SourceCatalogEntryPtr sourceCatalogEntry1 =
                    std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node8);
                SourceCatalogEntryPtr sourceCatalogEntry2 =
                    std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node9);
                SourceCatalogEntryPtr sourceCatalogEntry3 =
                    std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node16);
                SourceCatalogEntryPtr sourceCatalogEntry4 =
                    std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node15);
                SourceCatalogEntryPtr sourceCatalogEntry5 =
                    std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node6);
                SourceCatalogEntryPtr sourceCatalogEntry6 =
                    std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node11);
                SourceCatalogEntryPtr sourceCatalogEntry7 =
                    std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node12);
                SourceCatalogEntryPtr sourceCatalogEntry8 =
                    std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node13);

                //SourceCatalogEntryPtr sourceCatalogEntry9 =
                //    std::make_shared<SourceCatalogEntry>(physicalSource2, logicalSource2, node5);

                SourceCatalogEntryPtr sourceCatalogEntry10 =
                    std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node5);

                sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
                sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry2);
                sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry3);
                sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry4);
                sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry5);
                sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry6);
                sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry7);
                sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry8);

                sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry10);
                //sourceCatalog->addPhysicalSource(sourceName2, sourceCatalogEntry9);

                globalExecutionPlan = GlobalExecutionPlan::create();
                typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
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

TEST_F(QueryPlacementTest, getDepthTest){
    setupLargeTopologyAndSourceCatalog({4,4,4});

    setupSimpleQuery();

    EXPECT_EQ(Optimizer::BasePlacementStrategy::getDepth(globalExecutionPlan,globalExecutionPlan->getExecutionNodeByNodeId(9),0),3);
    EXPECT_EQ(Optimizer::BasePlacementStrategy::getDepth(globalExecutionPlan,globalExecutionPlan->getExecutionNodeByNodeId(10),0),2);
    EXPECT_EQ(Optimizer::BasePlacementStrategy::getDepth(globalExecutionPlan,globalExecutionPlan->getExecutionNodeByNodeId(16),0),4);
}

TEST_F(QueryPlacementTest, getNeighborsTest){
    setupLargeTopologyAndSourceCatalog({4,4,4});

    setupSimpleQuery();

    std::vector<ExecutionNodePtr> level1Nodes = {globalExecutionPlan->getExecutionNodeByNodeId(2), globalExecutionPlan->getExecutionNodeByNodeId(3)};

    std::vector<ExecutionNodePtr> level2Nodes = {globalExecutionPlan->getExecutionNodeByNodeId(4), globalExecutionPlan->getExecutionNodeByNodeId(5),
                                                 globalExecutionPlan->getExecutionNodeByNodeId(10), globalExecutionPlan->getExecutionNodeByNodeId(6),
                                                 globalExecutionPlan->getExecutionNodeByNodeId(7)};

    std::vector<ExecutionNodePtr> level3Nodes = {globalExecutionPlan->getExecutionNodeByNodeId(8), globalExecutionPlan->getExecutionNodeByNodeId(9),
                                                 globalExecutionPlan->getExecutionNodeByNodeId(14), globalExecutionPlan->getExecutionNodeByNodeId(15),
                                                 globalExecutionPlan->getExecutionNodeByNodeId(11), globalExecutionPlan->getExecutionNodeByNodeId(12),
                                                 globalExecutionPlan->getExecutionNodeByNodeId(13)};

    std::vector<ExecutionNodePtr> level4Nodes = {globalExecutionPlan->getExecutionNodeByNodeId(16)};


    EXPECT_EQ(Optimizer::BasePlacementStrategy::getNeighborNodes(globalExecutionPlan->getRootNodes()[0],0,1), level1Nodes);
    EXPECT_EQ(Optimizer::BasePlacementStrategy::getNeighborNodes(globalExecutionPlan->getRootNodes()[0],0,2), level2Nodes);
    EXPECT_EQ(Optimizer::BasePlacementStrategy::getNeighborNodes(globalExecutionPlan->getRootNodes()[0],0,3), level3Nodes);
    EXPECT_EQ(Optimizer::BasePlacementStrategy::getNeighborNodes(globalExecutionPlan->getRootNodes()[0],0,4), level4Nodes);
}

TEST_F(QueryPlacementTest, getDownstreamNeighborsTest){
    setupLargeTopologyAndSourceCatalog({4,4,4});
    setupSimpleQuery();

    std::vector<ExecutionNodePtr> downstreamNeighborsOf16 = {globalExecutionPlan->getExecutionNodeByNodeId(8), globalExecutionPlan->getExecutionNodeByNodeId(9),
                                                             globalExecutionPlan->getExecutionNodeByNodeId(14), globalExecutionPlan->getExecutionNodeByNodeId(15),
                                                             globalExecutionPlan->getExecutionNodeByNodeId(11), globalExecutionPlan->getExecutionNodeByNodeId(12),
                                                             globalExecutionPlan->getExecutionNodeByNodeId(13)};


    EXPECT_EQ(Optimizer::BasePlacementStrategy::getNeighborNodes(globalExecutionPlan->getRootNodes()[0], 0, (
                                                                                    (Optimizer::BasePlacementStrategy::getDepth(globalExecutionPlan, globalExecutionPlan->getExecutionNodeByNodeId(16),0) - 1)
                                                                                                            )), downstreamNeighborsOf16);
}

TEST_F(QueryPlacementTest, calcOutputQueueTest){
    setupSmallTopologyAndSourceCatalog({4,4,4});
    setupSimpleQuery();

    for(auto& node : executionNodes){
        NES_INFO("\nOutput Queue of node#" + std::to_string(node->getId()) + ": " + std::to_string(Optimizer::BasePlacementStrategy::calcOutputQueue(topology, node)));
    }



}

TEST_F(QueryPlacementTest, calcEffectiveValuesTest){
    setupLargeTopologyAndSourceCatalog({4,4,4});

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    NES_INFO("\n"+globalExecutionPlan->getAsString())

    std::vector<TopologyNodePtr> topologyNodes = std::vector<TopologyNodePtr>();
    topologyNodes.push_back(topology->getRoot());

    for(auto& child : topology->getRoot()->getAndFlattenAllChildren(false)){
        TopologyNodePtr nodeAsTopologyNode = child->as<TopologyNode>();
        topologyNodes.push_back(child->as<TopologyNode>());
    }
    Optimizer::BasePlacementStrategy::initAdjustedCosts(topology, globalExecutionPlan,globalExecutionPlan->getRootNodes()[0],queryId);

    NES_INFO("ADJUSTED COSTS: " + std::to_string(globalExecutionPlan->getExecutionNodeByNodeId(8)->getAdjustedCostsByQueryId(queryId)));

}

TEST_F(QueryPlacementTest, calcConnectivitiesTest){
    setupLargeTopologyAndSourceCatalog({4,4,4});
    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    NES_INFO("\n"+globalExecutionPlan->getAsString())

    Optimizer::BasePlacementStrategy::initNetworkConnectivities(topology,globalExecutionPlan,queryId);

    for(auto& node : globalExecutionPlan->getExecutionNodesByQueryId(queryId)){
        for(auto& connectivity : topology->findNodeWithId(node->getId())->connectivities){
            NES_INFO("\nCONNECTIVITY OF NODE#" + std::to_string(node->getId()) + " TO NODE#" + std::to_string(connectivity.first) + ": " + std::to_string(connectivity.second) + "ms")
        }
    }

}

TEST_F(QueryPlacementTest, calcLinkWeightTest){

    setupSmallTopologyAndSourceCatalog({4,4,4});
    setupSimpleQuery();

    ASSERT_EQ(executionNodes.size(), globalExecutionPlan->getExecutionNodesByQueryId(queryId).size());

    Optimizer::BasePlacementStrategy::initNetworkConnectivities(topology, globalExecutionPlan, queryId);

    for(auto& node : executionNodes) {
        for (auto& connectivity : topology->findNodeWithId(node->getId())->connectivities) {
            NES_INFO("\nCONNECTIVITY OF NODE#" + std::to_string(node->getId()) + " TO NODE#" + std::to_string(connectivity.first)
                     + ": " + std::to_string(connectivity.second) + "ms");

            NES_INFO("\nLINK WEIGHT OF NODE#" + std::to_string(node->getId()) + " TO NODE#" + std::to_string(connectivity.first)
                     + ": "
                     + std::to_string(Optimizer::BasePlacementStrategy::calcLinkWeight(
                         topology,
                         node,
                         globalExecutionPlan->getExecutionNodeByNodeId(connectivity.first))));
        }
    }
}

TEST_F(QueryPlacementTest, calcActiveStandbyTest){
    setupLargeTopologyAndSourceCatalog({4,4,4});
    setupSimpleQuery();

    Optimizer::BasePlacementStrategy::initAdjustedCosts(topology, globalExecutionPlan, globalExecutionPlan->getExecutionNodeByNodeId(1), queryId);
    Optimizer::BasePlacementStrategy::initNetworkConnectivities(topology, globalExecutionPlan, queryId);

    int replicas = 3;

    for(auto& node : executionNodes){

        std::tuple<float,float,float> activeStandbyResult =
            Optimizer::BasePlacementStrategy::calcActiveStandby(topology,globalExecutionPlan,node,replicas,queryId);

        float procCost = std::get<0>(activeStandbyResult);
        float networkingCost = std::get<1>(activeStandbyResult);
        float memoryCost = std::get<2>(activeStandbyResult);

        if((procCost + networkingCost + memoryCost) != -3){
            NES_INFO("\nACTIVE STANDBY COST OF NODE#" + std::to_string(node->getId()) + ": [" +
                     std::to_string(procCost) + ", " + std::to_string(networkingCost) +
                     ", " + std::to_string(memoryCost) + "]");
        }
    }


}

TEST_F(QueryPlacementTest, calcUpstreamBackupTest){
    topology = Topology::create();

    TopologyNodePtr node1 = TopologyNode::create(1, "localhost", 123, 124, 20);
    topology->setAsRoot(node1);

    TopologyNodePtr node2 = TopologyNode::create(2, "localhost", 125, 126, 15);
    TopologyNodePtr node3 = TopologyNode::create(3, "localhost", 125, 126, 16);
    TopologyNodePtr node4 = TopologyNode::create(4, "localhost", 125, 126, 11);
    TopologyNodePtr node5 = TopologyNode::create(5, "localhost", 125, 126, 12);
    TopologyNodePtr node6 = TopologyNode::create(6, "localhost", 125, 126, 10);
    TopologyNodePtr node7 = TopologyNode::create(7, "localhost", 125, 126, 13);
    TopologyNodePtr node8 = TopologyNode::create(8, "localhost", 125, 126, 7);
    TopologyNodePtr node9 = TopologyNode::create(9, "localhost", 125, 126, 6);
    TopologyNodePtr node10 = TopologyNode::create(10, "localhost", 125, 126, 9);
    TopologyNodePtr node11 = TopologyNode::create(11, "localhost", 125, 126, 6);
    TopologyNodePtr node12 = TopologyNode::create(12, "localhost", 125, 126, 5);
    TopologyNodePtr node13 = TopologyNode::create(13, "localhost", 125, 126, 7);
    TopologyNodePtr node14 = TopologyNode::create(14, "localhost", 125, 126, 4);
    TopologyNodePtr node15 = TopologyNode::create(15, "localhost", 125, 126, 4);
    TopologyNodePtr node16 = TopologyNode::create(16, "localhost", 125, 126, 2);


    topology->addNewTopologyNodeAsChild(node1, node2);
    topology->addNewTopologyNodeAsChild(node1, node3);

    topology->addNewTopologyNodeAsChild(node2, node4);
    topology->addNewTopologyNodeAsChild(node2, node5);

    topology->addNewTopologyNodeAsChild(node3, node6);
    topology->addNewTopologyNodeAsChild(node3, node7);
    topology->addNewTopologyNodeAsChild(node3, node10);

    topology->addNewTopologyNodeAsChild(node4, node8);
    topology->addNewTopologyNodeAsChild(node4, node9);
    topology->addNewTopologyNodeAsChild(node4, node10);

    topology->addNewTopologyNodeAsChild(node5, node9);
    topology->addNewTopologyNodeAsChild(node5, node10);

    topology->addNewTopologyNodeAsChild(node7, node11);
    topology->addNewTopologyNodeAsChild(node7, node12);
    topology->addNewTopologyNodeAsChild(node7, node13);

    topology->addNewTopologyNodeAsChild(node10, node14);
    topology->addNewTopologyNodeAsChild(node10, node15);

    topology->addNewTopologyNodeAsChild(node14, node16);

    topology->addNewTopologyNodeAsChild(node7, node15);



    std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64)"
                         "->addField(\"timestamp\", DataTypeFactory::createUInt64());";;
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<SourceCatalog>(queryParsingService);
    sourceCatalog->addLogicalSource(sourceName, schema);
    auto logicalSource = sourceCatalog->getLogicalSource(sourceName);


    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType->setGatheringInterval(0);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType2->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    auto physicalSource = PhysicalSource::create(sourceName, "test1", csvSourceType);

    SourceCatalogEntryPtr sourceCatalogEntry1 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node8);
    SourceCatalogEntryPtr sourceCatalogEntry2 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node9);
    SourceCatalogEntryPtr sourceCatalogEntry3 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node16);
    SourceCatalogEntryPtr sourceCatalogEntry4 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node15);
    SourceCatalogEntryPtr sourceCatalogEntry5 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node6);
    SourceCatalogEntryPtr sourceCatalogEntry6 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node11);
    SourceCatalogEntryPtr sourceCatalogEntry7 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node12);
    SourceCatalogEntryPtr sourceCatalogEntry8 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node13);

    SourceCatalogEntryPtr sourceCatalogEntry10 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node5);

    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry2);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry3);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry4);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry5);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry6);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry7);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry8);

    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry10);

    globalExecutionPlan = GlobalExecutionPlan::create();
    typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45)
                      .filter(Attribute("id") < 40)
                      .project(Attribute("id").as("id_new"), Attribute("value"))
                      .map(Attribute("value") = Attribute("value") + 1)
                      .as("carRename")
                      .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    Query subQuery1 = Query::from("car");


    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);
    executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    NES_INFO("\n"+globalExecutionPlan->getAsString())

    Optimizer::BasePlacementStrategy::initAdjustedCosts(topology, globalExecutionPlan, globalExecutionPlan->getExecutionNodeByNodeId(1), queryId);
    Optimizer::BasePlacementStrategy::initNetworkConnectivities(topology, globalExecutionPlan, queryId);

    for(auto& node : executionNodes){

        std::tuple<float,float,float> activeStandbyResult =
            Optimizer::BasePlacementStrategy::calcUpstreamBackup(topology,globalExecutionPlan,node,queryId);

        float procCost = std::get<0>(activeStandbyResult);
        float networkingCost = std::get<1>(activeStandbyResult);
        float memoryCost = std::get<2>(activeStandbyResult);

        if((procCost + networkingCost + memoryCost) != -3){
            NES_INFO("\nUPSTREAM BACKUP COST OF NODE#" + std::to_string(node->getId()) + ": [" +
                     std::to_string(procCost) + ", " + std::to_string(networkingCost) +
                     ", " + std::to_string(memoryCost) + "]");
        }
    }
}

TEST_F(QueryPlacementTest, calcCheckpointingTest){
    topology = Topology::create();

    TopologyNodePtr node1 = TopologyNode::create(1, "localhost", 123, 124, 20);
    topology->setAsRoot(node1);

    TopologyNodePtr node2 = TopologyNode::create(2, "localhost", 125, 126, 15);
    TopologyNodePtr node3 = TopologyNode::create(3, "localhost", 125, 126, 16);
    TopologyNodePtr node4 = TopologyNode::create(4, "localhost", 125, 126, 11);
    TopologyNodePtr node5 = TopologyNode::create(5, "localhost", 125, 126, 12);
    TopologyNodePtr node6 = TopologyNode::create(6, "localhost", 125, 126, 10);
    TopologyNodePtr node7 = TopologyNode::create(7, "localhost", 125, 126, 13);
    TopologyNodePtr node8 = TopologyNode::create(8, "localhost", 125, 126, 7);
    TopologyNodePtr node9 = TopologyNode::create(9, "localhost", 125, 126, 6);
    TopologyNodePtr node10 = TopologyNode::create(10, "localhost", 125, 126, 9);
    TopologyNodePtr node11 = TopologyNode::create(11, "localhost", 125, 126, 6);
    TopologyNodePtr node12 = TopologyNode::create(12, "localhost", 125, 126, 5);
    TopologyNodePtr node13 = TopologyNode::create(13, "localhost", 125, 126, 7);
    TopologyNodePtr node14 = TopologyNode::create(14, "localhost", 125, 126, 4);
    TopologyNodePtr node15 = TopologyNode::create(15, "localhost", 125, 126, 4);
    TopologyNodePtr node16 = TopologyNode::create(16, "localhost", 125, 126, 2);


    topology->addNewTopologyNodeAsChild(node1, node2);
    topology->addNewTopologyNodeAsChild(node1, node3);

    topology->addNewTopologyNodeAsChild(node2, node4);
    topology->addNewTopologyNodeAsChild(node2, node5);

    topology->addNewTopologyNodeAsChild(node3, node6);
    topology->addNewTopologyNodeAsChild(node3, node7);
    topology->addNewTopologyNodeAsChild(node3, node10);

    topology->addNewTopologyNodeAsChild(node4, node8);
    topology->addNewTopologyNodeAsChild(node4, node9);
    topology->addNewTopologyNodeAsChild(node4, node10);

    topology->addNewTopologyNodeAsChild(node5, node9);
    topology->addNewTopologyNodeAsChild(node5, node10);

    topology->addNewTopologyNodeAsChild(node7, node11);
    topology->addNewTopologyNodeAsChild(node7, node12);
    topology->addNewTopologyNodeAsChild(node7, node13);

    topology->addNewTopologyNodeAsChild(node10, node14);
    topology->addNewTopologyNodeAsChild(node10, node15);

    topology->addNewTopologyNodeAsChild(node14, node16);

    topology->addNewTopologyNodeAsChild(node7, node15);



    std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64)"
                         "->addField(\"timestamp\", DataTypeFactory::createUInt64());";;
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<SourceCatalog>(queryParsingService);
    sourceCatalog->addLogicalSource(sourceName, schema);
    auto logicalSource = sourceCatalog->getLogicalSource(sourceName);


    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType->setGatheringInterval(0);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType2->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    auto physicalSource = PhysicalSource::create(sourceName, "test1", csvSourceType);

    SourceCatalogEntryPtr sourceCatalogEntry1 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node8);
    SourceCatalogEntryPtr sourceCatalogEntry2 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node9);
    SourceCatalogEntryPtr sourceCatalogEntry3 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node16);
    SourceCatalogEntryPtr sourceCatalogEntry4 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node15);
    SourceCatalogEntryPtr sourceCatalogEntry5 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node6);
    SourceCatalogEntryPtr sourceCatalogEntry6 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node11);
    SourceCatalogEntryPtr sourceCatalogEntry7 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node12);
    SourceCatalogEntryPtr sourceCatalogEntry8 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node13);

    SourceCatalogEntryPtr sourceCatalogEntry10 =
        std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node5);

    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry2);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry3);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry4);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry5);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry6);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry7);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry8);

    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry10);

    globalExecutionPlan = GlobalExecutionPlan::create();
    typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45)
                      .filter(Attribute("id") < 40)
                      .project(Attribute("id").as("id_new"), Attribute("value"))
                      .map(Attribute("value") = Attribute("value") + 1)
                      .as("carRename")
                      .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    Query subQuery1 = Query::from("car");


    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);
    executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    NES_INFO("\n"+globalExecutionPlan->getAsString())

    Optimizer::BasePlacementStrategy::initAdjustedCosts(topology, globalExecutionPlan, globalExecutionPlan->getExecutionNodeByNodeId(1), queryId);
    Optimizer::BasePlacementStrategy::initNetworkConnectivities(topology, globalExecutionPlan, queryId);

    for(auto& node : executionNodes){

        std::tuple<float,float,float> activeStandbyResult =
            Optimizer::BasePlacementStrategy::calcCheckpointing(topology,globalExecutionPlan,node,queryId);

        float procCost = std::get<0>(activeStandbyResult);
        float networkingCost = std::get<1>(activeStandbyResult);
        float memoryCost = std::get<2>(activeStandbyResult);

        if((procCost + networkingCost + memoryCost) != -3){
            NES_INFO("\nCHECKPOINTING COST OF NODE#" + std::to_string(node->getId()) + ": [" +
                     std::to_string(procCost) + ", " + std::to_string(networkingCost) +
                     ", " + std::to_string(memoryCost) + "]");
        }
    }
}

TEST_F(QueryPlacementTest, calcActiveStandbyMergedQuery){

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    auto topologySpecificReWrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   NES::Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerRule::create(context);
    auto globalQueryPlan = GlobalQueryPlan::create();

    auto queryPlan1 = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create()).getQueryPlan();
    queryPlan1->setQueryId(1);

    queryPlan1 = queryReWritePhase->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan1);

    globalQueryPlan->addQueryPlan(queryPlan1);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    std::shared_ptr<QueryPlan> planToDeploy = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, true);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, updatedSharedQMToDeploy[0]);
    updatedSharedQMToDeploy[0]->setStatus(SharedQueryPlanStatus::Deployed);

    // new Query
    auto queryPlan2 = Query::from("car")
                          .filter(Attribute("id") < 45)
                          .map(Attribute("newId") = 2)
                          .sink(PrintSinkDescriptor::create())
                          .getQueryPlan();
    queryPlan2->setQueryId(2);

    queryPlan2 = queryReWritePhase->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    z3InferencePhase->execute(queryPlan2);

    globalQueryPlan->addQueryPlan(queryPlan2);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, updatedSharedQMToDeploy[0]);

}

TEST_F(QueryPlacementTest, calcActiveStandbyMergingTest){
    setupLargeTopologyAndSourceCatalog({4,4,4});

    GlobalExecutionPlanPtr globalExecutionPlanQuery1 = globalExecutionPlan;
    GlobalExecutionPlanPtr globalExecutionPlanQuery2 = globalExecutionPlan;

    int replicas = 4;

    Query query1 = Query::from("car").filter(Attribute("id") < 45)
                  .filter(Attribute("id") < 40)
                  .project(Attribute("id").as("id_new"), Attribute("value"))
                  .map(Attribute("value") = Attribute("value") + 1)
                  .as("carRename")
                  .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan1 = queryReWritePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan1);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan1);

    auto sharedQueryPlan1 = SharedQueryPlan::create(queryPlan1);
    QueryId queryId1 = sharedQueryPlan1->getSharedQueryId();
    auto queryPlacementPhase1 =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlanQuery1, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase1->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan1);
    std::vector<ExecutionNodePtr> executionNodesQuery1 = globalExecutionPlanQuery1->getExecutionNodesByQueryId(queryId1);

    Optimizer::BasePlacementStrategy::initAdjustedCosts(topology, globalExecutionPlanQuery1, globalExecutionPlanQuery1->getExecutionNodeByNodeId(1), queryId1);
    Optimizer::BasePlacementStrategy::initNetworkConnectivities(topology, globalExecutionPlanQuery1, queryId1);


    NES_INFO("\nGLOBALEXECUTIONPLAN QUERY1:\n"+globalExecutionPlanQuery1->getAsString());

    for(auto& node : executionNodesQuery1){

        std::tuple<float,float,float> activeStandbyResult =
            Optimizer::BasePlacementStrategy::calcActiveStandby(topology,globalExecutionPlanQuery1,node,replicas,queryId1);

        float procCost = std::get<0>(activeStandbyResult);
        float networkingCost = std::get<1>(activeStandbyResult);
        float memoryCost = std::get<2>(activeStandbyResult);

        if((procCost + networkingCost + memoryCost) != -3){
            NES_INFO("\nQUERY1: ACTIVE STANDBY COST OF NODE#" + std::to_string(node->getId()) + ": [" +
                     std::to_string(procCost) + ", " + std::to_string(networkingCost) +
                     ", " + std::to_string(memoryCost) + "]");
        }
    }

    for(auto&node : executionNodesQuery1){
        TopologyNodePtr topNode = topology->findNodeWithId(node->getId());
        topNode->reduceResources((topNode->getUsedResources()));
    }

    auto typeInferencePhase2 = typeInferencePhase;

    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();


    queryPlan2 = queryReWritePhase->execute(queryPlan2);
    typeInferencePhase2->execute(queryPlan2);

    topologySpecificQueryRewrite->execute(queryPlan2);
    typeInferencePhase2->execute(queryPlan2);

    auto sharedQueryPlan2 = SharedQueryPlan::create(queryPlan2);
    QueryId queryId2 = sharedQueryPlan2->getSharedQueryId();
    auto queryPlacementPhase2 =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlanQuery2, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase2->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan2);
    std::vector<ExecutionNodePtr> executionNodesQuery2 = globalExecutionPlanQuery2->getExecutionNodesByQueryId(queryId2);

    Optimizer::BasePlacementStrategy::initAdjustedCosts(topology, globalExecutionPlanQuery2, globalExecutionPlanQuery2->getExecutionNodeByNodeId(1), queryId2);
    Optimizer::BasePlacementStrategy::initNetworkConnectivities(topology, globalExecutionPlanQuery2, queryId2);


    NES_INFO("\nGLOBALEXECUTIONPLAN QUERY2:\n"+globalExecutionPlanQuery2->getAsString());

    for(auto& node : executionNodesQuery2){

        std::tuple<float,float,float> activeStandbyResult =
            Optimizer::BasePlacementStrategy::calcActiveStandby(topology,globalExecutionPlanQuery2,node,replicas,queryId2);

        float procCost = std::get<0>(activeStandbyResult);
        float networkingCost = std::get<1>(activeStandbyResult);
        float memoryCost = std::get<2>(activeStandbyResult);

        if((procCost + networkingCost + memoryCost) != -3){
            NES_INFO("\nQUERY2: ACTIVE STANDBY COST OF NODE#" + std::to_string(node->getId()) + ": [" +
                     std::to_string(procCost) + ", " + std::to_string(networkingCost) +
                     ", " + std::to_string(memoryCost) + "]");
        }
    }
}

/*TEST_F(QueryPlacementTest, calcUpstreamBackupTest){
    setupLargeTopologyAndSourceCatalog({4,4,4});
    setupSimpleQuery();

    Optimizer::BasePlacementStrategy::initAdjustedCosts(globalExecutionPlan, queryId);
    Optimizer::BasePlacementStrategy::calcAdjustedCosts(topology, globalExecutionPlan, globalExecutionPlan->getExecutionNodeByNodeId(1), queryId);
    Optimizer::BasePlacementStrategy::initNetworkConnectivities(topology, globalExecutionPlan, queryId);

    for(auto& node : executionNodes){

        std::tuple<float,float,float> UpstreamBackupResult =
            Optimizer::BasePlacementStrategy::calc

        float procCost = std::get<0>(activeStandbyResult);
        float networkingCost = std::get<1>(activeStandbyResult);
        float memoryCost = std::get<2>(activeStandbyResult);

        if((procCost + networkingCost + memoryCost) != -3){
            NES_INFO("\nACTIVE STANDBY COST OF NODE#" + std::to_string(node->getId()) + ": [" +
                     std::to_string(procCost) + ", " + std::to_string(networkingCost) +
                     ", " + std::to_string(memoryCost) + "]");
        }
    }
}*/