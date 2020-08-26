// clang-format off
#include <gtest/gtest.h>
// clang-format on
#include <API/Query.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>
#include <iostream>

using namespace NES;

class LogicalSourceExpansionRuleTest : public testing::Test {

  public:
    SchemaPtr schema;

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("LogicalSourceExpansionRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_INFO("Tear down LogicalSourceExpansionRuleTest test class.");
    }
};

void setupSensorNodeAndStreamCatalog(StreamCatalogPtr streamCatalog) {
    NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
    TopologyNodePtr physicalNode1 = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    TopologyNodePtr physicalNode2 = TopologyNode::create(2, "localhost", 4000, 4002, 4);

    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "test2";
    streamConf.logicalStreamName = "test_stream";

    StreamCatalogEntryPtr sce1 = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode1);
    StreamCatalogEntryPtr sce2 = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode2);
    streamCatalog->addPhysicalStream("default_logical", sce1);
    streamCatalog->addPhysicalStream("default_logical", sce2);
}

TEST_F(LogicalSourceExpansionRuleTest, testPushingOneFilterBelowMap) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    const std::string logicalStreamName = "default_logical";
    Query query = Query::from(logicalStreamName)
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") < 45)
                      .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();

    // Execute
    LogicalSourceExpansionRulePtr logicalSourceExpansionRule = LogicalSourceExpansionRule::create(streamCatalog);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    EXPECT_TRUE(streamCatalog->getSourceNodesForLogicalStream(logicalStreamName).size() == 2);
    EXPECT_TRUE(updatedPlan->getSourceOperators().size() == 2);
}

