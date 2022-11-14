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
#include <Runtime/BufferManager.hpp>
#include <Runtime/BufferStorage.hpp>
#include <FaultTolerance//FaultToleranceConfiguration.hpp>


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
            FaultToleranceConfigurationPtr ftConfig;
            Runtime::BufferManagerPtr bufferManager;
            Runtime::BufferStoragePtr bufferStorage;

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
                ftConfig = FaultToleranceConfiguration::create();
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

                //bufferManager = std::make_shared<Runtime::BufferManager>(1024,16);
                //bufferStorage = std::make_shared<Runtime::BufferStorage>();

                NES_INFO("\n"+globalExecutionPlan->getAsString())
            }

            void setupFirstSimpleCarQuery(){
                Query query1 = Query::from("car").filter(Attribute("id") < 45)
                                   .map(Attribute("value") = Attribute("value") + 1)
                                  .sink(PrintSinkDescriptor::create());
                QueryPlanPtr queryPlan = query1.getQueryPlan();




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



                NES_INFO("\n"+globalExecutionPlan->getAsString())
            }

            void setupSecondSimpleCarQuery(){
                Query query2 = Query::from("car").filter(Attribute("id") < 45)
                                   .map(Attribute("value") = Attribute("value") + 1)
                                   .sink(PrintSinkDescriptor::create());
                QueryPlanPtr queryPlan2 = query2.getQueryPlan();

                auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
                queryPlan2 = queryReWritePhase->execute(queryPlan2);

                typeInferencePhase->execute(queryPlan2);

                auto topologySpecificQueryRewrite =
                    Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
                topologySpecificQueryRewrite->execute(queryPlan2);

                typeInferencePhase->execute(queryPlan2);

                auto queryPlacementPhase =
                    Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);

                auto sharedQueryPlan2 = SharedQueryPlan::create(queryPlan2);
                queryId2 = sharedQueryPlan2->getSharedQueryId();
                queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan2);
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

                globalExecutionPlan->getExecutionNodeByNodeId(6)->setAvailableBandwidth(6250);
                globalExecutionPlan->getExecutionNodeByNodeId(8)->setAvailableBandwidth(6250);
                globalExecutionPlan->getExecutionNodeByNodeId(9)->setAvailableBandwidth(6250);
                globalExecutionPlan->getExecutionNodeByNodeId(11)->setAvailableBandwidth(6250);
                globalExecutionPlan->getExecutionNodeByNodeId(12)->setAvailableBandwidth(6250);
                globalExecutionPlan->getExecutionNodeByNodeId(13)->setAvailableBandwidth(6250);
                globalExecutionPlan->getExecutionNodeByNodeId(15)->setAvailableBandwidth(6250);
                globalExecutionPlan->getExecutionNodeByNodeId(16)->setAvailableBandwidth(6250);

                globalExecutionPlan->getExecutionNodeByNodeId(4)->setAvailableBandwidth(250000);
                globalExecutionPlan->getExecutionNodeByNodeId(5)->setAvailableBandwidth(250000);
                globalExecutionPlan->getExecutionNodeByNodeId(7)->setAvailableBandwidth(250000);
                globalExecutionPlan->getExecutionNodeByNodeId(14)->setAvailableBandwidth(250000);

                globalExecutionPlan->getExecutionNodeByNodeId(10)->setAvailableBandwidth(1000000);

                globalExecutionPlan->getExecutionNodeByNodeId(2)->setAvailableBandwidth(2000000);
                globalExecutionPlan->getExecutionNodeByNodeId(3)->setAvailableBandwidth(2000000);


                NES_INFO("\n"+globalExecutionPlan->getAsString())
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

            void setupTopology1(){
                topology = Topology::create();

                TopologyNodePtr node1 = TopologyNode::create(1, "localhost", 123, 124, 5);
                topology->setAsRoot(node1);

                TopologyNodePtr node2 = TopologyNode::create(2,"localhost", 125, 126, 3);
                topology->addNewTopologyNodeAsChild(node1, node2);

                TopologyNodePtr node3 = TopologyNode::create(3,"localhost", 127, 128, 1);
                topology->addNewTopologyNodeAsChild(node2, node3);

                std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                                     "->addField(\"sepLength\", BasicType::FLOAT64)"
                                     "->addField(\"sepWidth\", BasicType::FLOAT64)"
                                     "->addField(\"petLength\", BasicType::FLOAT64)"
                                     "->addField(\"petWidth\", BasicType::FLOAT64);";
                const std::string sourceName = "iris";

                sourceCatalog = std::make_shared<SourceCatalog>(queryParsingService);
                sourceCatalog->addLogicalSource(sourceName, schema);
                auto logicalSource = sourceCatalog->getLogicalSource(sourceName);

                CSVSourceTypePtr csvSourceType = CSVSourceType::create();
                csvSourceType->setGatheringInterval(12);
                csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
                auto physicalSource = PhysicalSource::create(sourceName, "irisPhysical", csvSourceType);

                SourceCatalogEntryPtr sourceCatalogEntry = std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node3);

                sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry);

                globalExecutionPlan = GlobalExecutionPlan::create();
                typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
            }

            void setupQuery1(){
                Query query = Query::from("iris").filter(Attribute("id") < 45)
                                  .filter(Attribute("id") < 40)
                                  .project(Attribute("id").as("id_new"), Attribute("petLength"), Attribute("petWidth"), Attribute("sepLength"), Attribute("sepWidth"))
                                  .filter(Attribute("sepWidth") < 2.5)
                                  .map(Attribute("sum") = Attribute("sepLength") * Attribute("petWidth"))
                                  .as("irisRename")
                                  .sink(PrintSinkDescriptor::create());
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

                globalExecutionPlan->getExecutionNodeByNodeId(2)->setAvailableBandwidth(2000000);
                topology->findNodeWithId(globalExecutionPlan->getExecutionNodeByNodeId(2)->getId())->setAvailableBuffers(5120);

                globalExecutionPlan->getExecutionNodeByNodeId(3)->setAvailableBandwidth(1000000);
                topology->findNodeWithId(globalExecutionPlan->getExecutionNodeByNodeId(3)->getId())->setAvailableBuffers(5120);



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
                                     "->addField(\"value\", BasicType::UINT64)"
                                     "->addField(\"age\", BasicType::UINT64)"
                                     "->addField(\"color\", BasicType::CHAR);";
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
                                     "->addField(\"age\", BasicType::UINT64)"
                                     "->addField(\"color\", BasicType::CHAR)"
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

    ASSERT_EQ(Optimizer::BasePlacementStrategy::getDepth(globalExecutionPlan->getExecutionNodeByNodeId(1)),0);
    ASSERT_EQ(Optimizer::BasePlacementStrategy::getDepth(globalExecutionPlan->getExecutionNodeByNodeId(9)),3);
    ASSERT_EQ(Optimizer::BasePlacementStrategy::getDepth(globalExecutionPlan->getExecutionNodeByNodeId(10)),2);
    ASSERT_EQ(Optimizer::BasePlacementStrategy::getDepth(globalExecutionPlan->getExecutionNodeByNodeId(16)),4);
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


    ASSERT_EQ(Optimizer::BasePlacementStrategy::getNeighborNodes(globalExecutionPlan->getRootNodes()[0], 0, (
                                                                                    (Optimizer::BasePlacementStrategy::getDepth(globalExecutionPlan->getExecutionNodeByNodeId(16)) - 1)
                                                                                                            )), downstreamNeighborsOf16);
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

    //Optimizer::BasePlacementStrategy::initNetworkConnectivities(topology,globalExecutionPlan,queryId);

    for(auto& node : globalExecutionPlan->getExecutionNodesByQueryId(queryId)){
        for(auto& connectivity : topology->findNodeWithId(node->getId())->connectivities){
            NES_INFO("\nCONNECTIVITY OF NODE#" + std::to_string(node->getId()) + " TO NODE#" + std::to_string(connectivity.first) + ": " + std::to_string(connectivity.second) + "ms")
        }
    }

}

TEST_F(QueryPlacementTest, firstFitPlacementTest){
    setupLargeTopologyAndSourceCatalog({4,4,4});
    setupSimpleQuery();


    globalExecutionPlan->getExecutionNodeByNodeId(6)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(8)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(9)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(11)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(12)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(13)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(15)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(16)->setAvailableBandwidth(6250);

    globalExecutionPlan->getExecutionNodeByNodeId(4)->setAvailableBandwidth(250000);
    globalExecutionPlan->getExecutionNodeByNodeId(5)->setAvailableBandwidth(250000);
    globalExecutionPlan->getExecutionNodeByNodeId(7)->setAvailableBandwidth(250000);
    globalExecutionPlan->getExecutionNodeByNodeId(14)->setAvailableBandwidth(250000);

    globalExecutionPlan->getExecutionNodeByNodeId(10)->setAvailableBandwidth(1000000);

    globalExecutionPlan->getExecutionNodeByNodeId(2)->setAvailableBandwidth(2000000);
    globalExecutionPlan->getExecutionNodeByNodeId(3)->setAvailableBandwidth(2000000);



    Optimizer::BasePlacementStrategy::initAdjustedCosts(topology, globalExecutionPlan, globalExecutionPlan->getExecutionNodeByNodeId(1), queryId);
    //Optimizer::BasePlacementStrategy::initNetworkConnectivities(topology, globalExecutionPlan, queryId);

    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setGatheringInterval(25);
    uint64_t epochRate = 18;
    int ack_size = 8;

    ftConfig->setIngestionRate(csvSourceType->getGatheringInterval()->getValue());
    ftConfig->setTupleSize(sourceCatalog->getSchemaForLogicalSource("car")->getSchemaSizeInBytes());
    ftConfig->setAckRate(18);
    ftConfig->setAckSize(8);
    ftConfig->setQueryId(queryId);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::for_each(executionNodes.begin(), executionNodes.end(),
                  [&](ExecutionNodePtr &executionNode){ topology->findNodeWithId(executionNode->getId())->setAvailableBuffers(4096);});


    std::vector<ExecutionNodePtr> executionNodesToUse;

    std::copy_if(executionNodes.begin(), executionNodes.end(), std::back_inserter(executionNodesToUse),
                 [](const ExecutionNodePtr executionNode) { return executionNode->getId() != 1; });



    std::vector<ExecutionNodePtr> sortedList =
        Optimizer::BasePlacementStrategy::getSortedListForFirstFit(executionNodesToUse, ftConfig, topology, globalExecutionPlan);

    NES_INFO("\nSORTED BY UB+AS COST:");
    for (size_t i = 0; i != sortedList.size(); ++i){
        NES_INFO("\n" << i << ". NODE#" << sortedList[i]->getId());
    }

    Optimizer::BasePlacementStrategy::firstFitRecursivePlacement(sortedList, ftConfig, topology, globalExecutionPlan);

    //std::pair<ExecutionNodePtr ,FaultToleranceType> result = Optimizer::BasePlacementStrategy::firstFitPlacement(sortedList, queryId, ftConfig, topology, globalExecutionPlan);

     //ExecutionNodePtr chosenNode = result.first;
     //FaultToleranceType chosenApproach = result.second;


     //NES_INFO("\nCHOSE NODE# " << chosenNode->getId() << " AND " << toString(chosenApproach));


    //std::vector<std::tuple<ExecutionNodePtr,FaultToleranceType,float>> placements = Optimizer::BasePlacementStrategy::firstFitRecursivePlacement(sortedList, ftConfig, topology);

    //NES_INFO("\nFT-PLACEMENTS:")
    //for(std::tuple<ExecutionNodePtr,FaultToleranceType,float> placement : placements){
    //    NES_INFO("\nON NODE#" << std::get<0>(placement)->getId() << " CHOOSE " << toString(std::get<1>(placement)) << " WITH COSTS OF " << std::get<2>(placement));
    //}



    //std::for_each(executionNodes.begin(), executionNodes.end(),
    //              [](ExecutionNodePtr &executionNode){ executionNode->increaseUsedBandwidth((25*20));});
}

TEST_F(QueryPlacementTest, firstFitTwoQueriesTest){
    setupLargeTopologyAndSourceCatalog({4,4,4});
    setupFirstSimpleCarQuery();
    setupSecondSimpleCarQuery();

    globalExecutionPlan->getExecutionNodeByNodeId(6)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(8)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(9)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(11)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(12)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(13)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(15)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(16)->setAvailableBandwidth(6250);

    globalExecutionPlan->getExecutionNodeByNodeId(4)->setAvailableBandwidth(250000);
    globalExecutionPlan->getExecutionNodeByNodeId(5)->setAvailableBandwidth(250000);
    globalExecutionPlan->getExecutionNodeByNodeId(7)->setAvailableBandwidth(250000);
    globalExecutionPlan->getExecutionNodeByNodeId(14)->setAvailableBandwidth(250000);

    globalExecutionPlan->getExecutionNodeByNodeId(10)->setAvailableBandwidth(1000000);

    globalExecutionPlan->getExecutionNodeByNodeId(2)->setAvailableBandwidth(2000000);
    globalExecutionPlan->getExecutionNodeByNodeId(3)->setAvailableBandwidth(2000000);

    std::for_each(globalExecutionPlan->getAllExecutionNodes().begin(), globalExecutionPlan->getAllExecutionNodes().end(),
                  [&](ExecutionNodePtr &executionNode){ topology->findNodeWithId(executionNode->getId())->setAvailableBuffers(4096);});

    Optimizer::BasePlacementStrategy::initAdjustedCosts(topology, globalExecutionPlan, globalExecutionPlan->getExecutionNodeByNodeId(1), queryId);
    //Optimizer::BasePlacementStrategy::initNetworkConnectivities(topology, globalExecutionPlan, queryId);

    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setGatheringInterval(25);
    uint64_t epochRate = 18;
    int ack_size = 8;

    ftConfig->setIngestionRate(csvSourceType->getGatheringInterval()->getValue());
    ftConfig->setTupleSize(sourceCatalog->getSchemaForLogicalSource("car")->getSchemaSizeInBytes());
    ftConfig->setAckRate(18);
    ftConfig->setAckSize(8);
    ftConfig->setQueryId(queryId);

    FaultToleranceConfigurationPtr  ftConfig2 = FaultToleranceConfiguration::create();
    ftConfig2->setIngestionRate(csvSourceType->getGatheringInterval()->getValue());
    ftConfig2->setTupleSize(sourceCatalog->getSchemaForLogicalSource("car")->getSchemaSizeInBytes());
    ftConfig2->setAckRate(18);
    ftConfig2->setAckSize(8);
    ftConfig2->setQueryId(queryId2);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    std::vector<ExecutionNodePtr> executionNodesQuery2 = globalExecutionPlan->getExecutionNodesByQueryId(queryId2);

    std::vector<ExecutionNodePtr> executionNodesToUse;
    std::vector<ExecutionNodePtr> executionNodesToUse2;

    std::copy_if(executionNodes.begin(), executionNodes.end(), std::back_inserter(executionNodesToUse),
                 [](const ExecutionNodePtr executionNode) { return executionNode->getId() != 1; });

    std::copy_if(executionNodesQuery2.begin(), executionNodesQuery2.end(), std::back_inserter(executionNodesToUse2),
                 [](const ExecutionNodePtr executionNode) { return executionNode->getId() != 1; });

    std::vector<ExecutionNodePtr> sortedList =
        Optimizer::BasePlacementStrategy::getSortedListForFirstFit(executionNodesToUse, ftConfig, topology, globalExecutionPlan);

    NES_INFO("\n1: SORTED BY UB+AS+CH COST:");
    for (size_t i = 0; i != sortedList.size(); ++i){
        NES_INFO("\n" << i << ". NODE#" << sortedList[i]->getId());
    }

    /*std::pair<ExecutionNodePtr ,FaultToleranceType> result = Optimizer::BasePlacementStrategy::firstFitPlacement(sortedList, queryId, ftConfig, topology, globalExecutionPlan);

    ExecutionNodePtr chosenNode = result.first;
    FaultToleranceType chosenApproach = result.second;
    NES_INFO("\nCHOSE NODE#" << chosenNode->getId() << " AND " << toString(chosenApproach));*/


    std::vector<ExecutionNodePtr> sortedListQuery2 =
        Optimizer::BasePlacementStrategy::getSortedListForFirstFit(executionNodesToUse2, ftConfig2, topology, globalExecutionPlan);

    NES_INFO("\n2: SORTED BY UB+AS+CH COST:");
    for (size_t i = 0; i != sortedListQuery2.size(); ++i){
        NES_INFO("\n" << i << ". NODE#" << sortedListQuery2[i]->getId());
    }
    /*std::pair<ExecutionNodePtr ,FaultToleranceType> result2 = Optimizer::BasePlacementStrategy::firstFitPlacement(sortedListQuery2, queryId2, ftConfig2, topology, globalExecutionPlan);

    ExecutionNodePtr chosenNode2 = result2.first;
    FaultToleranceType chosenApproach2 = result2.second;
    NES_INFO("\n2: CHOSE NODE#" << chosenNode2->getId() << " AND " << toString(chosenApproach2));*/


    //std::for_each(executionNodes.begin(), executionNodes.end(),
    //              [](ExecutionNodePtr &executionNode){ executionNode->increaseUsedBandwidth((25*20));});
}

TEST_F(QueryPlacementTest, firstFitMergingTest){
    setupLargeTopologyAndSourceCatalog({4,4,4});

    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setGatheringInterval(25);
    uint64_t epochRate = 18;
    int ack_size = 8;

    //QueryId 1
    setupFirstSimpleCarQuery();
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    std::vector<ExecutionNodePtr> executionNodesToUse = {};
    std::copy_if(executionNodes.begin(), executionNodes.end(), std::back_inserter(executionNodesToUse),
                 [](const ExecutionNodePtr executionNode) { return executionNode->getId() != 1; });

    ftConfig->setIngestionRate(csvSourceType->getGatheringInterval()->getValue());
    ftConfig->setTupleSize(sourceCatalog->getSchemaForLogicalSource("car")->getSchemaSizeInBytes());
    ftConfig->setAckRate(18);
    ftConfig->setAckSize(8);
    ftConfig->setQueryId(queryId);
    ftConfig->setProcessingGuarantee(FaultToleranceType::AT_LEAST_ONCE);

    globalExecutionPlan->getExecutionNodeByNodeId(6)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(8)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(9)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(11)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(12)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(13)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(15)->setAvailableBandwidth(6250);
    globalExecutionPlan->getExecutionNodeByNodeId(16)->setAvailableBandwidth(6250);

    globalExecutionPlan->getExecutionNodeByNodeId(4)->setAvailableBandwidth(250000);
    globalExecutionPlan->getExecutionNodeByNodeId(5)->setAvailableBandwidth(250000);
    globalExecutionPlan->getExecutionNodeByNodeId(7)->setAvailableBandwidth(250000);
    globalExecutionPlan->getExecutionNodeByNodeId(14)->setAvailableBandwidth(250000);

    globalExecutionPlan->getExecutionNodeByNodeId(10)->setAvailableBandwidth(1000000);

    globalExecutionPlan->getExecutionNodeByNodeId(2)->setAvailableBandwidth(2000000);
    globalExecutionPlan->getExecutionNodeByNodeId(3)->setAvailableBandwidth(2000000);

    std::for_each(executionNodes.begin(), executionNodes.end(),
                  [&](ExecutionNodePtr &executionNode){
                      topology->findNodeWithId(executionNode->getId())->setAvailableBuffers(5120);});


    std::for_each(executionNodesToUse.begin(), executionNodesToUse.end(),
                  [](ExecutionNodePtr &executionNode){ executionNode->increaseUsedBandwidth((20*25));});

    //QueryId 2
    setupSecondSimpleCarQuery();
    std::vector<ExecutionNodePtr> executionNodes2 = globalExecutionPlan->getExecutionNodesByQueryId(queryId2);
    std::vector<ExecutionNodePtr> executionNodesToUse2 = {};
    std::copy_if(executionNodes2.begin(), executionNodes2.end(), std::back_inserter(executionNodesToUse2),
                 [](const ExecutionNodePtr executionNode) { return executionNode->getId() != 1; });

    FaultToleranceConfigurationPtr  ftConfig2 = FaultToleranceConfiguration::create();
    ftConfig2->setIngestionRate(csvSourceType->getGatheringInterval()->getValue());
    ftConfig2->setTupleSize(sourceCatalog->getSchemaForLogicalSource("car")->getSchemaSizeInBytes());
    ftConfig2->setAckRate(30);
    ftConfig2->setAckSize(8);
    ftConfig2->setQueryId(queryId2);
    ftConfig2->setProcessingGuarantee(FaultToleranceType::EXACTLY_ONCE);

    std::for_each(executionNodesToUse2.begin(), executionNodesToUse2.end(),
                  [](ExecutionNodePtr &executionNode){ executionNode->increaseUsedBandwidth((20*25));});

    Optimizer::BasePlacementStrategy::initAdjustedCosts(topology, globalExecutionPlan, globalExecutionPlan->getExecutionNodeByNodeId(1), queryId);
    //Optimizer::BasePlacementStrategy::initNetworkConnectivities(topology, globalExecutionPlan, queryId);


    FaultToleranceType stricterProcessingGuarantee = Optimizer::BasePlacementStrategy::getStricterProcessingGuarantee(ftConfig, ftConfig2);
    ftConfig->setProcessingGuarantee(stricterProcessingGuarantee);
    ftConfig2->setProcessingGuarantee(stricterProcessingGuarantee);

    ExecutionNodePtr executionNodeToMergeOn = globalExecutionPlan->getExecutionNodeByNodeId(14);

    std::pair<FaultToleranceType,double> resultQuery1 = Optimizer::BasePlacementStrategy::getBestApproachForNode(executionNodeToMergeOn, executionNodesToUse, ftConfig, topology);
    std::pair<FaultToleranceType,double> resultQuery2 = Optimizer::BasePlacementStrategy::getBestApproachForNode(executionNodeToMergeOn, executionNodesToUse2, ftConfig2, topology);

    if(resultQuery1.second < resultQuery2.second){
        NES_INFO("\nMERGE ON NODE#" << executionNodeToMergeOn->getId() << " USING " << toString(resultQuery1.first) << " WITH COSTS OF " << resultQuery1.second << " [COMPARED TO " << resultQuery2.first
                 << " WITH COSTS OF " << resultQuery2.second << "]");
    }else{
        NES_INFO("\nMERGE ON NODE#" << executionNodeToMergeOn->getId() << " USING " << toString(resultQuery2.first) << " WITH COSTS OF " << resultQuery2.second << " [COMPARED TO " << resultQuery1.first
                                  << " WITH COSTS OF " << resultQuery1.second << "]");
    }

    //std::for_each(executionNodes.begin(), executionNodes.end(),
    //              [](ExecutionNodePtr &executionNode){ executionNode->increaseUsedBandwidth((25*20));});
}

TEST_F(QueryPlacementTest, iteratorTest){

    std::map<int,int> testMap = {{1,81},{2,65},{3,3},{4,5},{5,18},{6,78},{7,13},{8,3},{9,2},{10,11}};

    auto x = std::max_element(testMap.begin(), testMap.end(),
                              [](const std::pair<int, int>& p1, const std::pair<int, int>& p2) {
                                  return p1.second < p2.second; });

    ASSERT_TRUE(x->second == 81);

    auto y = std::max_element(testMap.begin(), testMap.end(),
                              [](const std::pair<int, int>& p1, const std::pair<int, int>& p2) {
                                  return p1.second > p2.second; });

    ASSERT_TRUE(y->second == 2);
}

TEST_F(QueryPlacementTest, sandboxTests){

    std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64)"
                         "->addField(\"timestamp\", DataTypeFactory::createUInt64());";;
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<SourceCatalog>(queryParsingService);
    sourceCatalog->addLogicalSource(sourceName, schema);
    auto logicalSource = sourceCatalog->getLogicalSource(sourceName);

    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setGatheringInterval(25);
    uint64_t epochRate = 18;
    int ack_size = 8;

    ftConfig->setIngestionRate(csvSourceType->getGatheringInterval()->getValue());
    ftConfig->setTupleSize(sourceCatalog->getSchemaForLogicalSource("car")->getSchemaSizeInBytes());
    ftConfig->setAckRate(18);
    ftConfig->setAckSize(8);

    auto physicalSource = PhysicalSource::create(sourceName, "test1", csvSourceType);

    NES_INFO("\nTUPLE SIZE: " <<sourceCatalog->getSchemaForLogicalSource("car")->getSchemaSizeInBytes() << " BYTE");
    NES_INFO("\nINGESTION RATE: " <<csvSourceType->getGatheringInterval()->getValue());
    NES_INFO("\nACK | CHK INTERVAL: EVERY " << epochRate << ". TUPLE");
    NES_INFO("\nACKNOWLEDGEMENT TUPLE SIZE: " << ack_size << " BYTE");

    //ASSERT_EQ(ftConfig->getCheckpointSize(), (18 * 20));
    //ASSERT_EQ(ftConfig->getAckInterval(), 25 / 18);

    std::vector<int> v1 = {1,2,3,4,5,6};



    std::transform(v1.begin(), v1.end(), v1.begin(),
                   [](const int i) { return i*2; });

    for(int i : v1){
        NES_INFO("INTY " << i);
    }
    //sourceCatalog->getSchemaForLogicalSource("car").

}

TEST_F(QueryPlacementTest, calcActiveStandbyMergedQuery){
    topology = Topology::create();

    TopologyNodePtr node1 = TopologyNode::create(1, "localhost", 123, 124, 40);
    topology->setAsRoot(node1);

    TopologyNodePtr node2 = TopologyNode::create(2, "localhost", 125, 126, 30);
    TopologyNodePtr node3 = TopologyNode::create(3, "localhost", 127, 128, 32);
    TopologyNodePtr node4 = TopologyNode::create(4, "localhost", 129, 130, 22);
    TopologyNodePtr node5 = TopologyNode::create(5, "localhost", 131, 132, 24);
    TopologyNodePtr node6 = TopologyNode::create(6, "localhost", 133, 134, 20);
    TopologyNodePtr node7 = TopologyNode::create(7, "localhost", 135, 136, 26);
    TopologyNodePtr node8 = TopologyNode::create(8, "localhost", 137, 138, 14);
    TopologyNodePtr node9 = TopologyNode::create(9, "localhost", 139, 140, 12);
    TopologyNodePtr node10 = TopologyNode::create(10, "localhost", 141, 142, 19);
    TopologyNodePtr node11 = TopologyNode::create(11, "localhost", 143, 144, 12);
    TopologyNodePtr node12 = TopologyNode::create(12, "localhost", 145, 146, 10);
    TopologyNodePtr node13 = TopologyNode::create(13, "localhost", 147, 148, 14);
    TopologyNodePtr node14 = TopologyNode::create(14, "localhost", 149, 150, 8);
    TopologyNodePtr node15 = TopologyNode::create(15, "localhost", 151, 152, 8);
    TopologyNodePtr node16 = TopologyNode::create(16, "localhost", 153, 154, 4);


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

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    auto topologySpecificReWrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   NES::Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerRule::create(context);
    auto globalQueryPlan = GlobalQueryPlan::create();

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    queryPlan1 = queryReWritePhase->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan1);

    globalQueryPlan->addQueryPlan(queryPlan1);

    queryPlan2 = queryReWritePhase->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    z3InferencePhase->execute(queryPlan2);

    globalQueryPlan->addQueryPlan(queryPlan2);


    //execute
    auto syntaxBasedEqualQueryMergerRule = Optimizer::SyntaxBasedCompleteQueryMergerRule::create();
    syntaxBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    std::shared_ptr<QueryPlan> planToDeploy = updatedSharedQMToDeploy[0]->getQueryPlan();

    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, true);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, updatedSharedQMToDeploy[0]);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, updatedSharedQMToDeploy[1]);

    NES_INFO("GEP:: " + globalExecutionPlan->getAsString());
    NES_INFO("SQMToDeploy size: " + std::to_string(updatedSharedQMToDeploy.size()));
    NES_INFO("SQP 1: " + updatedSharedQMToDeploy[0]->getQueryPlan()->toString());
    NES_INFO("SQP 2: " + updatedSharedQMToDeploy[1]->getQueryPlan()->toString());

}

TEST_F(QueryPlacementTest, mergerTest){


    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("truck").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto syntaxBasedEqualQueryMergerRule = Optimizer::SyntaxBasedCompleteQueryMergerRule::create();
    syntaxBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (const auto& sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (const auto& sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

TEST_F(QueryPlacementTest, finalTest1){
    setupTopology1();
    setupQuery1();

    Optimizer::BasePlacementStrategy::initAdjustedCosts(topology, globalExecutionPlan, globalExecutionPlan->getExecutionNodeByNodeId(1), queryId);

    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setGatheringInterval(12);
    uint64_t epochRate = 18;
    int ack_size = 8;

    ftConfig->setIngestionRate(csvSourceType->getGatheringInterval()->getValue());
    ftConfig->setTupleSize(sourceCatalog->getSchemaForLogicalSource("iris")->getSchemaSizeInBytes());
    ftConfig->setAckRate(18);
    ftConfig->setAckSize(8);
    ftConfig->setProcessingGuarantee(FaultToleranceType::AT_LEAST_ONCE);
    ftConfig->setQueryId(queryId);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //std::vector<ExecutionNodePtr> executionNodesToUse = {globalExecutionPlan->getExecutionNodeByNodeId(2), globalExecutionPlan->getExecutionNodeByNodeId(3)};

    //std::vector<ExecutionNodePtr> sortedList =
    //    Optimizer::BasePlacementStrategy::getSortedListForFirstFit(executionNodesToUse, ftConfig, topology, globalExecutionPlan);

    /*NES_INFO("\nSORTED BY UB+AS COST:");
    for (size_t i = 0; i != sortedList.size(); ++i){
        NES_INFO("\n" << i << ". NODE#" << sortedList[i]->getId());
    }

    Optimizer::BasePlacementStrategy::firstFitRecursivePlacement(sortedList, ftConfig, topology, globalExecutionPlan);*/



    std::vector<ExecutionNodePtr> sourceNodes = {globalExecutionPlan->getExecutionNodeByNodeId(3)};
    std::vector<ExecutionNodePtr> restNodes = {globalExecutionPlan->getExecutionNodeByNodeId(2)};
    std::vector<ExecutionNodePtr> nodesToUse = {globalExecutionPlan->getExecutionNodeByNodeId(3),
                                                globalExecutionPlan->getExecutionNodeByNodeId(2)};

    std::vector<FaultToleranceType> sortedApproaches = Optimizer::BasePlacementStrategy::getSortedApproachList(sourceNodes, nodesToUse, ftConfig, topology);

    for (size_t i = 0; i != sortedApproaches.size(); ++i){
        NES_INFO("\n" << i << ". APPROACH: " << toString(sortedApproaches[i]));
    }

    FaultToleranceType placedFT = Optimizer::BasePlacementStrategy::firstFitQueryPlacement(executionNodes, ftConfig, topology);

    NES_INFO("\nFOR QUERY#" << ftConfig->getQueryId() << ", CHOOSE " << toString(placedFT));

}

TEST_F(QueryPlacementTest, getRootNodeTest){
    setupTopology1();
    setupQuery1();



    ASSERT_EQ(Optimizer::BasePlacementStrategy::getExecutionNodeRootNode(globalExecutionPlan->getExecutionNodeByNodeId(3)),
              globalExecutionPlan->getExecutionNodeByNodeId(1));
}

/**
 * Setup:
 * Ingestion rate: 2000 tuples per second
 * Neighbor nodes are part of the same cluster and connected via a 1GBps router
 * Sensor layer bandwidth: 50kbps
 *      Node 8
 *      Node 9
 *      Node 6
 *      Node 11
 *      Node 12
 *      Node 13
 *      Node 15
 *      Node 16
 * Entry nodes: 2Mbps
 *      Node 4
 *      Node 5
 *      Node 7
 *      Node 14
 * Routing nodes:8MBps
 *      Node 10
 * Exit nodes: 16Mbps
 *      Node 2
 *      Node 3
 */

