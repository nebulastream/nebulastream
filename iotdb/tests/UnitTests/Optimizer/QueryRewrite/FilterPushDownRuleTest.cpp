#include <gtest/gtest.h>
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
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("FilterPushDownRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup FilterPushDownRuleTest test case.");
        setupSensorNodeAndStreamCatalog();
    }

    void setupSensorNodeAndStreamCatalog() {
        NES_INFO("Setup FilterPushDownRuleTest test case.");
        NESTopologySensorNodePtr sensorNode = NESTopologyManager::getInstance()
            .createNESSensorNode(1, "localhost", CPUCapacity::HIGH);

        PhysicalStreamConfig streamConf;
        streamConf.physicalStreamName = "test2";
        streamConf.logicalStreamName = "test_stream";

        StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, sensorNode);

        StreamCatalog::instance().addPhysicalStream("default_logical", sce);

        schema = Schema::create()->addField("id", BasicType::UINT32)->addField(
            "value", BasicType::UINT64);
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

TEST_F(FilterPushDownRuleTest, testPushingFilterBelowWindow) {

    Stream def = Stream("default_logical", schema);
    PrintSinkDescriptorPtr printSinkDescriptor = std::make_shared<PrintSinkDescriptor>(schema);
    Query query = Query::from(def).map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    ConsoleDumpHandler::create()->dump(queryPlan->getRootOperator(), std::cout);

    FilterPushDownRule filterPushDownRule;

    const QueryPlanPtr updatedPlan = filterPushDownRule.apply(queryPlan);

    ConsoleDumpHandler::create()->dump(updatedPlan->getRootOperator(), std::cout);


}

