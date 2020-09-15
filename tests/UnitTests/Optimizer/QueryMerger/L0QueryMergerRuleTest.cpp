// clang-format off
#include <gtest/gtest.h>
// clang-format on
#include <API/Query.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
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

TEST_F(L0QueryMergerRuleTest, testPushingOneFilterBelowMap) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("default_logical")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    queryPlan1->setQueryId(UtilityFunctions::getNextQueryId());

    Query query2 = Query::from("default_logical")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    queryPlan2->setQueryId(UtilityFunctions::getNextQueryId());

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    globalQueryPlan->removeQuery(1);
}
