#include <gtest/gtest.h>
#include <iostream>
#include <Util/Logger.hpp>
#include <API/Query.hpp>
#include <Topology/TopologyManager.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>

using namespace NES;

class FilterPushDownTest : public testing::Test {

  public:
    SchemaPtr schema;

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("FilterPushDownTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup FilterPushDownTest test case.");
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField(
            "value", BasicType::UINT64);
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

void setupSensorNodeAndStreamCatalog(TopologyManagerPtr topologyManager , StreamCatalogPtr streamCatalog) {
    NES_INFO("Setup FilterPushDownTest test case.");
    NESTopologySensorNodePtr sensorNode = topologyManager->createNESSensorNode(1, "localhost", CPUCapacity::HIGH);

    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "test2";
    streamConf.logicalStreamName = "test_stream";

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, sensorNode);
    streamCatalog->addPhysicalStream("default_logical", sce);
}

TEST_F(FilterPushDownTest, testPushingOneFilterBelowMap) {
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(topologyManager, streamCatalog);

    Query::streamCatalog = streamCatalog;
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
        .map(Attribute("value") = 40)
        .filter(Attribute("id") < 45)
        .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperator());
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    FilterPushDownRule filterPushDownRule;
    const QueryPlanPtr updatedPlan = filterPushDownRule.apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperator());
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownTest, testPushingOneFilterBelowMapAndBeforeFilter) {
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(topologyManager, streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
        .filter(Attribute("id") > 45)
        .map(Attribute("value") = 40)
        .filter(Attribute("id") < 45)
        .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperator());
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator1 = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    FilterPushDownRule filterPushDownRule;
    const QueryPlanPtr updatedPlan = filterPushDownRule.apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperator());
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownTest, testPushingFiltersBelowAllMapOperators) {
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(topologyManager, streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
        .map(Attribute("value") = 80)
        .filter(Attribute("id") > 45)
        .map(Attribute("value") = 40)
        .filter(Attribute("id") < 45)
        .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperator());
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator1 = (*itr);
    ++itr;
    const NodePtr mapOperator1 = (*itr);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    ++itr;
    const NodePtr mapOperator2 = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    FilterPushDownRule filterPushDownRule;
    const QueryPlanPtr updatedPlan = filterPushDownRule.apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperator());
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownTest, testPushingTwoFilterBelowMap) {
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(topologyManager, streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
        .map(Attribute("value") = 40)
        .filter(Attribute("id") > 45)
        .filter(Attribute("id") < 45)
        .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperator());
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator1 = (*itr);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    FilterPushDownRule filterPushDownRule;
    const QueryPlanPtr updatedPlan = filterPushDownRule.apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperator());
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownTest, testPushingFilterAlreadyAtBottom) {
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(topologyManager, streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
        .filter(Attribute("id") > 45)
        .map(Attribute("value") = 40)
        .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperator());
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    FilterPushDownRule filterPushDownRule;
    const QueryPlanPtr updatedPlan = filterPushDownRule.apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperator());
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}
