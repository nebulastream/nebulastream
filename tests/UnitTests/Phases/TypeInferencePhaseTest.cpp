#include <API/Query.hpp>
#include <API/Window/TimeCharacteristic.hpp>
#include <API/Window/WindowAggregation.hpp>
#include <API/Window/WindowType.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/PhysicalNode.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>

namespace NES {

class TypeInferencePhaseTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup TypeInferencePhaseTest test class." << std::endl;
    }

    /* Will be called before a  test is executed. */
    void SetUp() {
        NES::setupLogging("TypeInferencePhaseTest.log", NES::LOG_DEBUG);
        std::cout << "Setup TypeInferencePhaseTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Tear down TypeInferencePhaseTest test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down TypeInferencePhaseTest test class." << std::endl;
    }
};

/**
 * @brief In this test we infer the output and input schemas of each operator in a query.
 */
TEST_F(TypeInferencePhaseTest, inferQueryPlan) {
    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    auto source = createSourceLogicalOperatorNode(DefaultSourceDescriptor::create(inputSchema, "default_logical", 0, 0));
    auto map = createMapLogicalOperatorNode(Attribute("f3") = Attribute("f1") * 42);
    auto sink = createSinkLogicalOperatorNode(FileSinkDescriptor::create(""));

    auto plan = QueryPlan::create(source);
    plan->appendOperator(map);
    plan->appendOperator(sink);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    PhysicalNodePtr physicalNode = PhysicalNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "test2";
    streamConf.logicalStreamName = "test_stream";

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);
    streamCatalog->addPhysicalStream("default_logical", sce);

    auto phase = TypeInferencePhase::create(streamCatalog);
    auto resultPlan = phase->execute(plan);

    // we just access the old references

    ASSERT_TRUE(source->getOutputSchema()->equals(inputSchema));

    auto mappedSchema = Schema::create();
    mappedSchema->addField("f1", BasicType::INT32);
    mappedSchema->addField("f2", BasicType::INT8);
    mappedSchema->addField("f3", BasicType::INT8);

    ASSERT_TRUE(map->getOutputSchema()->equals(mappedSchema));
    ASSERT_TRUE(sink->getOutputSchema()->equals(mappedSchema));
}

/**
 * @brief In this test we infer the output and input schemas of each operator in a query.
 */
TEST_F(TypeInferencePhaseTest, inferWindowQuery) {

    auto query = Query::from("default_logical")
                     .window(
                         TumblingWindow::of(TimeCharacteristic::createProcessingTime(), Seconds(10)),
                         Sum::on(Attribute("value")))
                     .sink(FileSinkDescriptor::create(""));

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    PhysicalNodePtr physicalNode = PhysicalNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "test2";
    streamConf.logicalStreamName = "test_stream";

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);
    streamCatalog->addPhysicalStream("default_logical", sce);

    auto phase = TypeInferencePhase::create(streamCatalog);
    auto resultPlan = phase->execute(query.getQueryPlan());

    // we just access the old references

    ASSERT_TRUE(resultPlan->getSinkOperators()[0]->getOutputSchema()->getSize() == 1);
}

/**
 * @brief In this test we try to infer the output and input scheas of an invalid query. This should fail.
 */
TEST_F(TypeInferencePhaseTest, inferQueryPlanError) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    auto source = createSourceLogicalOperatorNode(DefaultSourceDescriptor::create(inputSchema, "default_logical", 0, 0));
    auto map = createMapLogicalOperatorNode(Attribute("f3") = Attribute("f3") * 42);
    auto sink = createSinkLogicalOperatorNode(FileSinkDescriptor::create(""));

    auto plan = QueryPlan::create(source);
    plan->appendOperator(map);
    plan->appendOperator(sink);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    PhysicalNodePtr physicalNode = PhysicalNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "test2";
    streamConf.logicalStreamName = "test_stream";

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);
    streamCatalog->addPhysicalStream("default_logical", sce);
    auto phase = TypeInferencePhase::create(streamCatalog);
    ASSERT_ANY_THROW(phase->execute(plan));
}

/**
 * @brief In this test we ensure that the source descriptor is correctly replaced, such that the schema can be propagated.
 */
TEST_F(TypeInferencePhaseTest, inferQuerySourceReplace) {

    PhysicalNodePtr physicalNode = PhysicalNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "test2";
    streamConf.logicalStreamName = "test_stream";

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);
    streamCatalog->addPhysicalStream("default_logical", sce);

    SchemaPtr schema = Schema::create()
                           ->addField("id", BasicType::UINT32)
                           ->addField("value", BasicType::UINT64);

    auto query = Query::from("default_logical")
                     .map(Attribute("f3") = Attribute("id")++)
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = TypeInferencePhase::create(streamCatalog);

    plan = phase->execute(plan);
    auto sink = plan->getSinkOperators()[0];

    auto resultSchema = Schema::create()
                            ->addField("id", BasicType::UINT32)
                            ->addField("value", BasicType::UINT64)
                            ->addField("f3", BasicType::UINT32);

    ASSERT_TRUE(sink->getOutputSchema()->equals(resultSchema));
}

}// namespace NES
