/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <gtest/gtest.h>

#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/IFCOPStrategy.hpp>
#include <Optimizer/QueryPlacement/GeneticAlgorithmOptimizer.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Phases/QueryRewritePhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>


using namespace NES;
using namespace web;

class IFCOPTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup IFCOPTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("IFCOPTest.log", NES::LOG_DEBUG);

    }

    /* Will be called before a test is executed. */
    void TearDown() { std::cout << "Setup IFCOPTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down IFCOPTest test class." << std::endl; }

    void setupTopologyAndStreamCatalog() {
        uint32_t grpcPort = 4000;
        uint32_t dataPort = 5000;

        topology = Topology::create();

        // creater workers
        std::vector<TopologyNodePtr> topologyNodes;
        int resource = 4;
        for (uint32_t i = 0; i < 11; ++i) {
            topologyNodes.push_back(TopologyNode::create(i, "localhost", grpcPort, dataPort, resource));
            grpcPort = grpcPort + 2;
            dataPort = dataPort + 2;
        }

        topology->setAsRoot(topologyNodes.at(0));

        // link each worker with its neighbor
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(1));
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(2));

        topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(3));
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(4));

        topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(5));
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(6));

        topology->addNewPhysicalNodeAsChild(topologyNodes.at(4), topologyNodes.at(7));
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(5), topologyNodes.at(7));

        topology->addNewPhysicalNodeAsChild(topologyNodes.at(7), topologyNodes.at(8));
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(7), topologyNodes.at(9));

        topology->addNewPhysicalNodeAsChild(topologyNodes.at(6), topologyNodes.at(10));


        std::string carSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                                "->addField(\"value\", BasicType::UINT64);";
        const std::string carStreamName = "car";

        std::string truckSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                                  "->addField(\"value\", BasicType::UINT64);";
        const std::string truckStreamName = "truck";

        streamCatalog = std::make_shared<StreamCatalog>();
        streamCatalog->addLogicalStream(carStreamName, carSchema);
        streamCatalog->addLogicalStream(truckStreamName, truckSchema);

        SourceConfigPtr carSourceConfig = SourceConfig::create();
        carSourceConfig->setSourceType("DefaultSource");
        carSourceConfig->setLogicalStreamName("car");
        carSourceConfig->setPhysicalStreamName("test2");
        carSourceConfig->setSourceConfig("1");
        carSourceConfig->setSourceFrequency(0);
        carSourceConfig->setNumberOfTuplesToProducePerBuffer(1);
        carSourceConfig->setNumberOfBuffersToProduce(1);
        carSourceConfig->setSkipHeader(false);

        PhysicalStreamConfigPtr confCar = PhysicalStreamConfig::create(carSourceConfig);

        SourceConfigPtr truckSourceConfig = SourceConfig::create();
        truckSourceConfig->setSourceType("DefaultSource");
        truckSourceConfig->setLogicalStreamName("car");
        truckSourceConfig->setPhysicalStreamName("test2");
        truckSourceConfig->setSourceConfig("1");
        truckSourceConfig->setSourceFrequency(0);
        truckSourceConfig->setNumberOfTuplesToProducePerBuffer(1);
        truckSourceConfig->setNumberOfBuffersToProduce(1);
        truckSourceConfig->setSkipHeader(false);

        PhysicalStreamConfigPtr confTruck = PhysicalStreamConfig::create(truckSourceConfig);

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(confCar, topologyNodes.at(8));
        StreamCatalogEntryPtr streamCatalogEntry2 = std::make_shared<StreamCatalogEntry>(confCar, topologyNodes.at(9));
        StreamCatalogEntryPtr streamCatalogEntry3 = std::make_shared<StreamCatalogEntry>(confTruck, topologyNodes.at(10));

        streamCatalog->addPhysicalStream("car", streamCatalogEntry1);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry2);
        streamCatalog->addPhysicalStream("truck", streamCatalogEntry3);
    }

    void setupSmallTopologyAndStreamCatalog() {
        topology = Topology::create();

        // create workers
        std::vector<TopologyNodePtr> topologyNodes;
        topologyNodes.push_back(TopologyNode::create(0, "localhost", 4000, 4000, 2));
        topologyNodes.push_back(TopologyNode::create(1, "localhost", 4002, 4002, 1));
        topologyNodes.push_back(TopologyNode::create(2, "localhost", 4004, 4004, 1));
        topologyNodes.push_back(TopologyNode::create(3, "localhost", 4004, 4004, 1));

        topology->setAsRoot(topologyNodes.at(0));

        // link each worker with its neighbor
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(1));
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(2));
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(3));

        std::string carSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                                "->addField(\"value\", BasicType::UINT64);";
        const std::string carStreamName = "car";

        streamCatalog = std::make_shared<StreamCatalog>();
        streamCatalog->addLogicalStream(carStreamName, carSchema);
        NES_DEBUG("IFCOPTest::setupSmallTopologyAndStreamCatalog() size of carSchema=" << streamCatalog->getSchemaForLogicalStream("car")->getSchemaSizeInBytes());


        SourceConfigPtr carSourceConfig = SourceConfig::create();
        carSourceConfig->setSourceType("DefaultSource");
        carSourceConfig->setLogicalStreamName("car");
        carSourceConfig->setPhysicalStreamName("test2");
        carSourceConfig->setSourceConfig("1");
        carSourceConfig->setSourceFrequency(0);
        carSourceConfig->setNumberOfTuplesToProducePerBuffer(1);
        carSourceConfig->setNumberOfBuffersToProduce(1);
        carSourceConfig->setSkipHeader(false);

        PhysicalStreamConfigPtr confCar = PhysicalStreamConfig::create(carSourceConfig);

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(confCar, topologyNodes.at(3));

        streamCatalog->addPhysicalStream("car", streamCatalogEntry1);
    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};

/* Test generating random execution path  */
TEST_F(IFCOPTest, costFunctionTest) {

    setupSmallTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    Query query = Query::from("car")
                        .filter(Attribute("id") > 1)
//                        .map(Attribute("id_new")=Attribute("id")+1)
                        .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
    queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto ifcop = IFCOPStrategy::create(globalExecutionPlan, topology, typeInferencePhase, streamCatalog);

    TopologyNodePtr rootOfOptimizedExecutionpath = ifcop->getOptimizedExecutionPath(topology, 5, queryPlan);
    auto nodeToOperatorMap = ifcop->getRandomAssignment(rootOfOptimizedExecutionpath, queryPlan->getSourceOperators());

    auto tupleSizeModifier = ifcop->getTupleSizeModifier(queryPlan);
    auto ingestionRateModifier = ifcop->getIngestionRateModifier(queryPlan);

    auto cost = ifcop->getTotalCost(queryPlan, topology, rootOfOptimizedExecutionpath, nodeToOperatorMap, tupleSizeModifier,
                                    ingestionRateModifier);
    EXPECT_EQ(cost, 48);
    NES_DEBUG("IFCOPTest: nodeToOperatorMap.size()=" << nodeToOperatorMap.size());
}

/* Test generating random execution path  */
TEST_F(IFCOPTest, geneticAlgorithmTest) {

    setupSmallTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    Query query = Query::from("car")
        .filter(Attribute("id") > 1)
//                        .map(Attribute("id_new")=Attribute("id")+1)
        .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
    queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto tupleSizeModifier = IFCOPStrategy::getTupleSizeModifier(queryPlan);
    auto ingestionRateModifier = IFCOPStrategy::getIngestionRateModifier(queryPlan);

    GeneticAlgorithmOptimizer geneticAlgorithmOptimizer = GeneticAlgorithmOptimizer(queryPlan, topology, 10, 0.5, 0.15, 2,
                                                                                    tupleSizeModifier,ingestionRateModifier);

    auto placement0 = geneticAlgorithmOptimizer.population[0];
    auto mutatedPlacement = geneticAlgorithmOptimizer.mutate(placement0);

    auto placement1 = geneticAlgorithmOptimizer.population[1];

    auto offspring = geneticAlgorithmOptimizer.crossOver(placement0, placement1);

    geneticAlgorithmOptimizer.getCost(placement1);

    auto bestPlacement = geneticAlgorithmOptimizer.getOptimizedPlacement(10);

    NES_DEBUG("Best placement found: \n" << GeneticAlgorithmOptimizer::getOperatorAssignmentAsString(bestPlacement));

    NES_DEBUG("IFCOPTest: geneticAlgorithmTest()");
}

///* Test generating random execution path  */
//TEST_F(IFCOPTest, testGeneratingRandomExecutionPath) {
//
//    setupTopologyAndStreamCatalog();
//
//    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
//    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);
//    auto subQuery = Query::from("truck");
//    Query query = Query::from("car").filter(Attribute("id") > 1)
//                      .merge(&subQuery)
//                      .sink(PrintSinkDescriptor::create());
//
//    QueryPlanPtr queryPlan = query.getQueryPlan();
//    QueryId queryId = PlanIdGenerator::getNextQueryId();
//    queryPlan->setQueryId(queryId);
//
//    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
//    queryReWritePhase->execute(queryPlan);
//    typeInferencePhase->execute(queryPlan);
//
//    auto ifcop = IFCOPStrategy::create(globalExecutionPlan, topology, typeInferencePhase, streamCatalog);
//
//    auto randomExecutionPath = ifcop->generateRandomExecutionPath(topology, queryPlan);
//
//    //check if the source nodes are in the execution path
//    bool isSourceOneFound = false;
//    bool isSourceTwoFound = false;
//    for (NodePtr child: randomExecutionPath->getAndFlattenAllChildren(false)){
//        isSourceOneFound = isSourceOneFound || child->as<TopologyNode>()->getId() == 8;
//        isSourceTwoFound = isSourceTwoFound || child->as<TopologyNode>()->getId() == 10;
//    }
//
//    ASSERT_TRUE(isSourceOneFound);
//    ASSERT_TRUE(isSourceTwoFound);
//}

///* Test generating random execution path  */
//TEST_F(IFCOPTest, testSelectOptimizedExecutionPath) {
//
//    setupTopologyAndStreamCatalog();
//
//    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
//    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);
//    auto subQuery = Query::from("truck");
//    Query query = Query::from("car").filter(Attribute("id") > 1).merge(&subQuery).sink(PrintSinkDescriptor::create());
//
//    QueryPlanPtr queryPlan = query.getQueryPlan();
//    QueryId queryId = PlanIdGenerator::getNextQueryId();
//    queryPlan->setQueryId(queryId);
//
//    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
//    queryReWritePhase->execute(queryPlan);
//    typeInferencePhase->execute(queryPlan);
//
//    auto ifcop = IFCOPStrategy::create(globalExecutionPlan, topology, typeInferencePhase, streamCatalog);
//
//    auto randomExecutionPath = ifcop->getOptimizedExecutionPath(topology, 5, queryPlan);
//}

//// TODO: Add test for operator assignment
///* Test to run operator assignment */
//TEST_F(IFCOPTest, testOperatorAssignment) {
//    // Get an execution path
//    setupTopologyAndStreamCatalog();
//
//    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
//    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);
//    auto subQuery = Query::from("truck");
//    Query query = Query::from("car").filter(Attribute("id") > 1).merge(&subQuery).sink(PrintSinkDescriptor::create());
//
//    QueryPlanPtr queryPlan = query.getQueryPlan();
//    QueryId queryId = PlanIdGenerator::getNextQueryId();
//    queryPlan->setQueryId(queryId);
//
//    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
//    queryReWritePhase->execute(queryPlan);
//    typeInferencePhase->execute(queryPlan);
//
//    auto ifcop = IFCOPStrategy::create(globalExecutionPlan, topology, typeInferencePhase, streamCatalog);
//
//    TopologyNodePtr rootOfOptimizedExecutionpath = ifcop->getOptimizedExecutionPath(topology, 5, queryPlan);
//
//    std::map<TopologyNodePtr,std::vector<LogicalOperatorNodePtr>> initialNodeToOperatorsMap;
//    auto nodeToOperatorMap = ifcop->getRandomAssignment(rootOfOptimizedExecutionpath, queryPlan->getSourceOperators());
//    NES_DEBUG("IFCOPTest: nodeToOperatorMap.size()=" << nodeToOperatorMap.size());
//}