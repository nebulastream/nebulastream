#include <gtest/gtest.h>
#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Phases/TypeInferencePhase.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Topology/NESTopologySensorNode.hpp>
#include <Topology/TopologyManager.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <memory>

namespace NES {

class TypeInferencePhaseTest : public testing::Test {
  public:
    void SetUp() {
    }

    void TearDown() {
        NES_DEBUG("Tear down TypeInferencePhase Test.");
    }

  protected:
    static void setupLogging() {
        NES::setupLogging("TypeInferencePhase.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup TypeInferencePhase test class.");
    }
};

/**
 * @brief In this test we infer the output and input schemas of each operator in a query.
 */
TEST_F(TypeInferencePhaseTest, inferQueryPlan) {
    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    auto source = createSourceLogicalOperatorNode(DefaultSourceDescriptor::create(inputSchema, 0, 0));
    auto map = createMapLogicalOperatorNode(Attribute("f3") = Attribute("f1") * 42);
    auto sink = createSinkLogicalOperatorNode(FileSinkDescriptor::create("", FILE_APPEND, CSV_TYPE));

    auto plan = QueryPlan::create(source);
    plan->appendOperator(map);
    plan->appendOperator(sink);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    NESTopologySensorNodePtr sensorNode = topologyManager->createNESSensorNode(1, "localhost", CPUCapacity::HIGH);

    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "test2";
    streamConf.logicalStreamName = "test_stream";

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf,
                                                                     sensorNode);
    streamCatalog->addPhysicalStream("default_logical", sce);

    auto phase = TypeInferencePhase::create();
    phase->setStreamCatalog(streamCatalog);

    auto resultPlan = phase->transform(plan);

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
 * @brief In this test we try to infer the output and input scheas of an invalid query. This should fail.
 */
TEST_F(TypeInferencePhaseTest, inferQueryPlanError) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    auto source = createSourceLogicalOperatorNode(DefaultSourceDescriptor::create(inputSchema, 0, 0));
    auto map = createMapLogicalOperatorNode(Attribute("f3") = Attribute("f3") * 42);
    auto sink = createSinkLogicalOperatorNode(FileSinkDescriptor::create("", FILE_APPEND, CSV_TYPE));

    auto plan = QueryPlan::create(source);
    plan->appendOperator(map);
    plan->appendOperator(sink);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    NESTopologySensorNodePtr sensorNode = topologyManager->createNESSensorNode(1, "localhost", CPUCapacity::HIGH);

    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "test2";
    streamConf.logicalStreamName = "test_stream";

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf,
                                                                     sensorNode);
    streamCatalog->addPhysicalStream("default_logical", sce);
    auto phase = TypeInferencePhase::create();
    phase->setStreamCatalog(streamCatalog);
    ASSERT_ANY_THROW(phase->transform(plan));
}

/**
 * @brief In this test we ensure that the source descriptor is correctly replaced, such that the schema can be propagated.
 */
TEST_F(TypeInferencePhaseTest, inferQuerySourceReplace) {

    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    NESTopologySensorNodePtr sensorNode = topologyManager->createNESSensorNode(1, "localhost", CPUCapacity::HIGH);

    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "test2";
    streamConf.logicalStreamName = "test_stream";

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf,
                                                                     sensorNode);
    streamCatalog->addPhysicalStream("default_logical", sce);

    SchemaPtr schema = Schema::create()
                           ->addField("id", BasicType::UINT32)
                           ->addField("value", BasicType::UINT64);

    auto query = Query::from("default_logical")
                     .map(Attribute("f3") = Attribute("id")++)
                     .sink(FileSinkDescriptor::create("", FILE_APPEND, CSV_TYPE));
    auto plan = query.getQueryPlan();

    auto phase = TypeInferencePhase::create();
    phase->setStreamCatalog(streamCatalog);

    plan = phase->transform(plan);
    auto sink = plan->getSinkOperators()[0];

    auto resultSchema = Schema::create()
                            ->addField("id", BasicType::UINT32)
                            ->addField("value", BasicType::UINT64)
                            ->addField("f3", BasicType::UINT32);

    ASSERT_TRUE(sink->getOutputSchema()->equals(resultSchema));
}

}// namespace NES
