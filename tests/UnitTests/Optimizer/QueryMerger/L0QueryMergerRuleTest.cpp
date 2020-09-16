// clang-format off
#include <gtest/gtest.h>
// clang-format on
#include <API/Query.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>
#include <Optimizer/QueryMerger/L0QueryMergerRule.hpp>
#include <iostream>
#include <Util/UtilityFunctions.hpp>

using namespace NES;

class L0QueryMergerRuleTest : public testing::Test {

  public:
    SchemaPtr schema;

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("FilterPushDownTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup FilterPushDownTest test case.");
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        NES_INFO("Setup FilterPushDownTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_INFO("Tear down FilterPushDownTest test class.");
    }
};

void setupSensorNodeAndStreamCatalog(StreamCatalogPtr streamCatalog) {
    NES_INFO("Setup FilterPushDownTest test case.");
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "test2";
    streamConf.logicalStreamName = "test_stream";

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);
    streamCatalog->addPhysicalStream("default_logical", sce);
}

TEST_F(L0QueryMergerRuleTest, testMergingEqualQueries) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("default_logical")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = UtilityFunctions::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("default_logical")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = UtilityFunctions::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperators()[0]->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperators()[0]->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_EQ(sink1GQNChild, sink2GQNChild);
        }
    }
}

TEST_F(L0QueryMergerRuleTest, testMergingQueriesWithDifferentFilters) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("default_logical")
        .map(Attribute("value") = 40)
        .filter(Attribute("id") < 45)
        .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = UtilityFunctions::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("default_logical")
        .map(Attribute("value") = 40)
        .filter(Attribute("id") < 40)
        .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = UtilityFunctions::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
      return sinkGQN->getOperators()[0]->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
      return sinkGQN->getOperators()[0]->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

TEST_F(L0QueryMergerRuleTest, testMergingQueriesWithDifferentFiltersField) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("default_logical")
        .map(Attribute("value") = 40)
        .filter(Attribute("id") < 40)
        .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = UtilityFunctions::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("default_logical")
        .map(Attribute("value") = 40)
        .filter(Attribute("id1") < 40)
        .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = UtilityFunctions::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
      return sinkGQN->getOperators()[0]->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
      return sinkGQN->getOperators()[0]->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

TEST_F(L0QueryMergerRuleTest, testMergingQueriesWithDifferentMapAttribute) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("default_logical")
        .map(Attribute("value") = 40)
        .filter(Attribute("id") < 40)
        .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = UtilityFunctions::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("default_logical")
        .map(Attribute("value1") = 40)
        .filter(Attribute("id") < 40)
        .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = UtilityFunctions::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
      return sinkGQN->getOperators()[0]->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
      return sinkGQN->getOperators()[0]->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

TEST_F(L0QueryMergerRuleTest, testMergingQueriesWithDifferentMapValue) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("default_logical")
        .map(Attribute("value") = 40)
        .filter(Attribute("id") < 40)
        .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = UtilityFunctions::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("default_logical")
        .map(Attribute("value") = 50)
        .filter(Attribute("id") < 40)
        .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = UtilityFunctions::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
      return sinkGQN->getOperators()[0]->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
      return sinkGQN->getOperators()[0]->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}