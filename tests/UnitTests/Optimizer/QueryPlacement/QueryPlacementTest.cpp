#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
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
#include <gtest/gtest.h>

using namespace NES;
using namespace web;

class QueryPlacementTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup QueryPlacementTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("QueryPlacementTest.log", NES::LOG_DEBUG);
        StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down QueryPlacementTest test class." << std::endl;
    }

    void setupTopologyAndStreamCatalog() {

        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", 123, 124, 4);
        topology->setAsRoot(rootNode);

        TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, 4);
        topology->addNewPhysicalNodeAsChild(rootNode, sourceNode1);

        TopologyNodePtr sourceNode2 = TopologyNode::create(3, "localhost", 123, 124, 4);
        topology->addNewPhysicalNodeAsChild(rootNode, sourceNode2);

        std::string schema =
            "Schema::create()->addField(\"id\", BasicType::UINT32)"
            "->addField(\"value\", BasicType::UINT64);";
        const std::string streamName = "car";

        streamCatalog = std::make_shared<StreamCatalog>();
        streamCatalog->addLogicalStream(streamName, schema);

        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(/**Source Type**/ "DefaultSource", /**Source Config**/ "1",
                                                                    /**Source Frequence**/ 0, /**Number Of Tuples To Produce Per Buffer**/ 0,
                                                                    /**Number of Buffers To Produce**/ 1, /**Physical Stream Name**/ "test2",
                                                                    /**Logical Stream Name**/ "car");

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, sourceNode1);
        StreamCatalogEntryPtr streamCatalogEntry2 = std::make_shared<StreamCatalogEntry>(conf, sourceNode2);

        streamCatalog->addPhysicalStream("car", streamCatalogEntry1);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry2);
    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};

/* Test query placement with bottom up strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithBottomUpStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = PlacementStrategyFactory::getStrategy("BottomUp", globalExecutionPlan, topology, typeInferencePhase, streamCatalog);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = UtilityFunctions::getNextQueryId();
    queryPlan->setQueryId(queryId);

    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
    queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3);
    for (auto executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2);
            for (auto children : actualRootOperator->getChildren()) {
                ASSERT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            ASSERT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (auto children : actualRootOperator->getChildren()) {
                ASSERT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
            }
        }
    }
}

/* Test query placement with top down strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithTopDownStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = PlacementStrategyFactory::getStrategy("TopDown", globalExecutionPlan, topology, typeInferencePhase, streamCatalog);

    Query query = Query::from("car")
            .filter(Attribute("id") < 45)
            .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = UtilityFunctions::getNextQueryId();
    queryPlan->setQueryId(queryId);

    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
    queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3);
    for (auto executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            std::vector<SourceLogicalOperatorNodePtr> sourceOperators = querySubPlan->getSourceOperators();
            ASSERT_EQ(sourceOperators.size(), 2);
            for (auto sourceOperator : sourceOperators) {
                ASSERT_TRUE(sourceOperator->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            ASSERT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (auto children : actualRootOperator->getChildren()) {
                ASSERT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test query placement of query with multiple sink with bottom up strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkWithBottomUpStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = PlacementStrategyFactory::getStrategy("BottomUp", globalExecutionPlan, topology, typeInferencePhase, streamCatalog);

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));
    sourceOperator->setId(UtilityFunctions::getNextOperatorId());

//    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
//    filterOperator->setId(UtilityFunctions::getNextOperatorId());
//    filterOperator->addChild(sourceOperator);

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);
    sinkOperator1->setId(UtilityFunctions::getNextOperatorId());

    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);
    sinkOperator2->setId(UtilityFunctions::getNextOperatorId());

    sinkOperator1->addChild(sourceOperator);
    sinkOperator2->addChild(sourceOperator);

    QueryPlanPtr queryPlan =  QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);
    QueryId queryId = UtilityFunctions::getNextQueryId();
    queryPlan->setQueryId(queryId);

    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
    queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3);
    for (auto executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2);
            for (auto children : actualRootOperator->getChildren()) {
                ASSERT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            ASSERT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (auto children : actualRootOperator->getChildren()) {
                ASSERT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
            }
        }
    }
}

///* Test nes topology service create plan for valid query string for  */
//TEST_F(QueryPlacementTest, create_nes_execution_plan_for_valid_query_using_topdown) {
//    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
//    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
//    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
//    OptimizerServicePtr optimizerService = std::make_shared<OptimizerService>(topologyManager, streamCatalog, globalExecutionPlan);
//    createExampleTopology(streamCatalog, topologyManager);
//    QueryServicePtr queryService = std::make_shared<QueryService>();
//
//    std::stringstream code;
//    code << "Query::from(\"temperature\").filter(Attribute(\"value\")==5)" << std::endl
//         << ".sink(ZmqSinkDescriptor::create(\"localhost\", 10));";
//    const QueryPtr query = queryService->getQueryFromQueryString(code.str());
//
//    const std::string plan = optimizerService->getExecutionPlanAsString(query->getQueryPlan(), "TopDown");
//    EXPECT_TRUE(plan.size() != 0);
//}
//
///* Test nes topology service create plan for invalid query string for  */
//TEST_F(QueryPlacementTest, create_nes_execution_plan_for_invalid_query) {
//    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
//    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
//    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
//    OptimizerServicePtr optimizerService = std::make_shared<OptimizerService>(topologyManager, streamCatalog, globalExecutionPlan);
//    createExampleTopology(streamCatalog, topologyManager);
//    QueryServicePtr queryService = std::make_shared<QueryService>();
//
//    try {
//        std::stringstream code;
//        code << "" << std::endl;
//        queryService->getQueryFromQueryString(code.str());
//        FAIL();
//    } catch (...) {
//        //TODO: We need to look into exception handling soon enough
//        SUCCEED();
//    }
//}
//
///* Test nes topology service create plan for invalid optimization strategy */
//TEST_F(QueryPlacementTest, create_nes_execution_plan_for_invalid_optimization_strategy) {
//    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
//    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
//    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
//    OptimizerServicePtr optimizerService = std::make_shared<OptimizerService>(topologyManager, streamCatalog, globalExecutionPlan);
//    createExampleTopology(streamCatalog, topologyManager);
//    QueryServicePtr queryService = std::make_shared<QueryService>();
//
//    try {
//        std::stringstream code;
//        code << "Query::from(\"temperature134\").filter(Attribute(\"id\")==5)"
//             << ".sink(ZmqSinkDescriptor::create(\"localhost\", 10));" << std::endl;
//
//        const QueryPtr query = queryService->getQueryFromQueryString(code.str());
//        const std::string plan = optimizerService->getExecutionPlanAsString(query->getQueryPlan(), "BottomUp");
//        FAIL();
//    } catch (...) {
//        //TODO: We need to look into exception handling soon enough
//        SUCCEED();
//    }
//
//    try {
//        //test wrong field in filter
//        std::stringstream code;
//        code
//            << "Query::from(temperature1).filter(Attribute(\"wrong_field\")==5)"
//            << std::endl
//            << ".sink(ZmqSinkDescriptor::create(\"localhost\", 10));"
//            << std::endl;
//
//        const QueryPtr query = queryService->getQueryFromQueryString(code.str());
//        const std::string plan = optimizerService->getExecutionPlanAsString(query->getQueryPlan(), "BottomUp");
//        FAIL();
//    } catch (...) {
//        //TODO: We need to look into exception handling soon enough
//        SUCCEED();
//    }
//}
