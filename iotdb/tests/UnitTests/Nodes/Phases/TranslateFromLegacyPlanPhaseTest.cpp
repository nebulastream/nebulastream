#include <gtest/gtest.h>
#include <iostream>
#include <API/Schema.hpp>
#include <Util/Logger.hpp>
#include <Topology/NESTopologySensorNode.hpp>
#include <Topology/TopologyManager.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <API/InputQuery.hpp>
#include <API/Query.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Phases/TranslateFromLegacyPlanPhase.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>

using namespace NES;

class TranslateFromLegacyPlanPhaseTest : public testing::Test {

  public:

    SchemaPtr schema;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES_INFO("Setup TranslateFromLegacyPlanPhase test class.");
        setupSensorNodeAndStreamCatalog();
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("TranslateFromLegacyPlanPhase.log", NES::LOG_DEBUG);
        NES_INFO("Setup TranslateFromLegacyPlanPhase test case.");
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField(
            "value", BasicType::UINT64);
    }

    void static setupSensorNodeAndStreamCatalog() {
        NES_INFO("Setup TranslateFromLegacyPlanPhase test case.");
        TopologyManagerPtr topologyManager = std::shared_ptr<TopologyManager>();

        NESTopologySensorNodePtr sensorNode = topologyManager->createNESSensorNode(1, "localhost", CPUCapacity::HIGH);

        PhysicalStreamConfig streamConf;
        streamConf.physicalStreamName = "test2";
        streamConf.logicalStreamName = "test_stream";

        StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, sensorNode);
        assert(0);
//        StreamCatalog::instance().addPhysicalStream("default_logical", sce);
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        NES_INFO("Setup TranslateFromLegacyPlanPhase test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_INFO("Tear down FilterPushDownRuleTest test class.");
    }
};

TEST_F(TranslateFromLegacyPlanPhaseTest, testTranslationOfInputQueryToLogicalPlan) {

    // Prepare
    Stream def = Stream("default_logical", schema);
    InputQuery input = InputQuery::from(def).print();

    //Execute
    auto translationPhase = TranslateFromLegacyPlanPhase::create();
    const OperatorNodePtr ptr = translationPhase->transform(input.getRoot());

    //Assert
    DepthFirstNodeIterator queryPlanNodeIterator(ptr);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr sourceOperator = (*itr);

    auto sinkLogicalOperator = sinkOperator->as<SinkLogicalOperatorNode>();
    auto srcLogicalOperator = sourceOperator->as<SourceLogicalOperatorNode>();
    EXPECT_TRUE(sinkLogicalOperator->getSinkDescriptor()->instanceOf<PrintSinkDescriptor>());
    EXPECT_TRUE( srcLogicalOperator->getSourceDescriptor()->getSchema()->equals(schema));
}

