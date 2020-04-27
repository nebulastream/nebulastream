#include <gtest/gtest.h>
#include <iostream>
#include <Util/Logger.hpp>
#include <API/Query.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Node.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>

using namespace NES;

class FilterPushDownRuleTest : public testing::Test {

  public:

    SchemaPtr schema;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES_INFO("Setup FilterPushDownRuleTest test class.");
        setupSensorNodeAndStreamCatalog();
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("FilterPushDownRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup FilterPushDownRuleTest test case.");
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField(
            "value", BasicType::UINT64);
    }

    void static setupSensorNodeAndStreamCatalog() {
        NES_INFO("Setup FilterPushDownRuleTest test case.");
        NESTopologySensorNodePtr sensorNode = NESTopologyManager::getInstance()
            .createNESSensorNode(1, "localhost", CPUCapacity::HIGH);

        PhysicalStreamConfig streamConf;
        streamConf.physicalStreamName = "test2";
        streamConf.logicalStreamName = "test_stream";

        StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, sensorNode);

        StreamCatalog::instance().addPhysicalStream("default_logical", sce);
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        NES_INFO("Setup FilterPushDownRuleTest test case.")
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_INFO("Tear down FilterPushDownRuleTest test class.")
    }
};

TEST_F(FilterPushDownRuleTest, testPushingOneFilterBelowMap) {

    // Prepare
    Stream def = Stream("default_logical", schema);
    PrintSinkDescriptorPtr printSinkDescriptor = std::make_shared<PrintSinkDescriptor>(schema);
    Query query = Query::from(def)
        .map(Attribute("value") = 40)
        .filter(Attribute("id") < 45)
        .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    const OperatorNodePtr sinkOperator = queryPlan->getRootOperator();
    const NodePtr filterOperator = sinkOperator->getChildren()[0];
    const NodePtr mapOperator = filterOperator->getChildren()[0];
    const NodePtr srcOperator = mapOperator->getChildren()[0];

    std::stringstream expectedOutput;
    expectedOutput << std::string(0, ' ') << sinkOperator->toString() << std::endl;
    expectedOutput << std::string(2, ' ') << mapOperator->toString() << std::endl;
    expectedOutput << std::string(4, ' ') << filterOperator->toString() << std::endl;
    expectedOutput << std::string(6, ' ') << srcOperator->toString() << std::endl;

    // Execute
    FilterPushDownRule filterPushDownRule;
    const QueryPlanPtr updatedPlan = filterPushDownRule.apply(queryPlan);

    // Validate
    std::stringstream actualOutput;
    ConsoleDumpHandler::create()->dump(updatedPlan->getRootOperator(), actualOutput);
    EXPECT_EQ(actualOutput.str(), expectedOutput.str());
}

TEST_F(FilterPushDownRuleTest, testPushingOneFilterBelowMapAndBeforeFilter) {

    // Prepare
    Stream def = Stream("default_logical", schema);
    PrintSinkDescriptorPtr printSinkDescriptor = std::make_shared<PrintSinkDescriptor>(schema);
    Query query = Query::from(def)
        .filter(Attribute("id") > 45)
        .map(Attribute("value") = 40)
        .filter(Attribute("id") < 45)
        .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    const OperatorNodePtr sinkOperator = queryPlan->getRootOperator();
    const NodePtr filterOperator1 = sinkOperator->getChildren()[0];
    const NodePtr mapOperator = filterOperator1->getChildren()[0];
    const NodePtr filterOperator2 = mapOperator->getChildren()[0];
    const NodePtr srcOperator = filterOperator2->getChildren()[0];

    std::stringstream expectedOutput;
    expectedOutput << std::string(0, ' ') << sinkOperator->toString() << std::endl;
    expectedOutput << std::string(2, ' ') << mapOperator->toString() << std::endl;
    expectedOutput << std::string(4, ' ') << filterOperator1->toString() << std::endl;
    expectedOutput << std::string(6, ' ') << filterOperator2->toString() << std::endl;
    expectedOutput << std::string(8, ' ') << srcOperator->toString() << std::endl;

    // Execute
    FilterPushDownRule filterPushDownRule;
    const QueryPlanPtr updatedPlan = filterPushDownRule.apply(queryPlan);

    // Validate
    std::stringstream actualOutput;
    ConsoleDumpHandler::create()->dump(updatedPlan->getRootOperator(), actualOutput);
    EXPECT_EQ(actualOutput.str(), expectedOutput.str());
}

TEST_F(FilterPushDownRuleTest, testPushingFiltersBelowAllMapOperators) {

    // Prepare
    Stream def = Stream("default_logical", schema);
    PrintSinkDescriptorPtr printSinkDescriptor = std::make_shared<PrintSinkDescriptor>(schema);
    Query query = Query::from(def)
        .map(Attribute("value") = 80)
        .filter(Attribute("id") > 45)
        .map(Attribute("value") = 40)
        .filter(Attribute("id") < 45)
        .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    const OperatorNodePtr sinkOperator = queryPlan->getRootOperator();
    const NodePtr filterOperator1 = sinkOperator->getChildren()[0];
    const NodePtr mapOperator1 = filterOperator1->getChildren()[0];
    const NodePtr filterOperator2 = mapOperator1->getChildren()[0];
    const NodePtr mapOperator2 = filterOperator2->getChildren()[0];
    const NodePtr srcOperator = mapOperator2->getChildren()[0];

    std::stringstream expectedOutput;
    expectedOutput << std::string(0, ' ') << sinkOperator->toString() << std::endl;
    expectedOutput << std::string(2, ' ') << mapOperator1->toString() << std::endl;
    expectedOutput << std::string(4, ' ') << mapOperator2->toString() << std::endl;
    expectedOutput << std::string(6, ' ') << filterOperator1->toString() << std::endl;
    expectedOutput << std::string(8, ' ') << filterOperator2->toString() << std::endl;
    expectedOutput << std::string(10, ' ') << srcOperator->toString() << std::endl;

    // Execute
    FilterPushDownRule filterPushDownRule;
    const QueryPlanPtr updatedPlan = filterPushDownRule.apply(queryPlan);

    // Validate
    std::stringstream actualOutput;
    ConsoleDumpHandler::create()->dump(updatedPlan->getRootOperator(), actualOutput);
    EXPECT_EQ(actualOutput.str(), expectedOutput.str());
}

TEST_F(FilterPushDownRuleTest, testPushingTwoFilterBelowMap) {

    // Prepare
    Stream def = Stream("default_logical", schema);
    PrintSinkDescriptorPtr printSinkDescriptor = std::make_shared<PrintSinkDescriptor>(schema);
    Query query = Query::from(def)
        .map(Attribute("value") = 40)
        .filter(Attribute("id") > 45)
        .filter(Attribute("id") < 45)
        .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    const OperatorNodePtr sinkOperator = queryPlan->getRootOperator();
    const NodePtr filterOperator1 = sinkOperator->getChildren()[0];
    const NodePtr filterOperator2 = filterOperator1->getChildren()[0];
    const NodePtr mapOperator = filterOperator2->getChildren()[0];
    const NodePtr srcOperator = mapOperator->getChildren()[0];

    std::stringstream expectedOutput;
    expectedOutput << std::string(0, ' ') << sinkOperator->toString() << std::endl;
    expectedOutput << std::string(2, ' ') << mapOperator->toString() << std::endl;
    expectedOutput << std::string(4, ' ') << filterOperator1->toString() << std::endl;
    expectedOutput << std::string(6, ' ') << filterOperator2->toString() << std::endl;
    expectedOutput << std::string(8, ' ') << srcOperator->toString() << std::endl;

    // Execute
    FilterPushDownRule filterPushDownRule;
    const QueryPlanPtr updatedPlan = filterPushDownRule.apply(queryPlan);

    // Validate
    std::stringstream actualOutput;
    ConsoleDumpHandler::create()->dump(updatedPlan->getRootOperator(), actualOutput);
    EXPECT_EQ(actualOutput.str(), expectedOutput.str());
}

TEST_F(FilterPushDownRuleTest, testPushingFilterAlreadyAtBottom) {

    // Prepare
    Stream def = Stream("default_logical", schema);
    PrintSinkDescriptorPtr printSinkDescriptor = std::make_shared<PrintSinkDescriptor>(schema);
    Query query = Query::from(def)
        .filter(Attribute("id") > 45)
        .map(Attribute("value") = 40)
        .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    const OperatorNodePtr sinkOperator = queryPlan->getRootOperator();
    const NodePtr mapOperator = sinkOperator->getChildren()[0];
    const NodePtr filterOperator = mapOperator->getChildren()[0];
    const NodePtr srcOperator = filterOperator->getChildren()[0];

    std::stringstream expectedOutput;
    expectedOutput << std::string(0, ' ') << sinkOperator->toString() << std::endl;
    expectedOutput << std::string(2, ' ') << mapOperator->toString() << std::endl;
    expectedOutput << std::string(4, ' ') << filterOperator->toString() << std::endl;
    expectedOutput << std::string(6, ' ') << srcOperator->toString() << std::endl;

    // Execute
    FilterPushDownRule filterPushDownRule;
    const QueryPlanPtr updatedPlan = filterPushDownRule.apply(queryPlan);

    // Validate
    std::stringstream actualOutput;
    ConsoleDumpHandler::create()->dump(updatedPlan->getRootOperator(), actualOutput);
    EXPECT_EQ(actualOutput.str(), expectedOutput.str());
}
