#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/InputQuery.hpp>
#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <API/Types/DataTypes.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <GRPC/Serialization/DataTypeSerializationUtil.hpp>
#include <GRPC/Serialization/ExpressionSerializationUtil.hpp>
#include <GRPC/Serialization/OperatorSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Phases/TranslateFromLegacyPlanPhase.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <QueryCompiler/DataTypes/ArrayDataType.hpp>
#include <SerializableOperator.pb.h>
#include <Topology/NESTopologySensorNode.hpp>
#include <Util/Logger.hpp>
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <iostream>
using namespace NES;

class SerializationUtilTest : public testing::Test {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES_INFO("Setup SerializationUtilTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("SerializationUtilTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SerializationUtilTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        NES_INFO("Setup SerializationUtilTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_INFO("Tear down SerializationUtilTest test class.");
    }
};

TEST_F(SerializationUtilTest, dataTypeSerialization) {

    // serialize and deserialize int8
    auto serializedInt8 = DataTypeSerializationUtil::serializeDataType(createDataType(INT8), new SerializableDataType());
    auto deserializedInt8 = DataTypeSerializationUtil::deserializeDataType(serializedInt8);
    ASSERT_TRUE(createDataType(INT8)->isEqual(deserializedInt8));

    // serialize and deserialize int16
    auto serializedInt16 = DataTypeSerializationUtil::serializeDataType(createDataType(INT16), new SerializableDataType());
    auto deserializedInt16 = DataTypeSerializationUtil::deserializeDataType(serializedInt16);
    ASSERT_TRUE(createDataType(INT16)->isEqual(deserializedInt16));

    // serialize and deserialize int32
    auto serializedInt32 = DataTypeSerializationUtil::serializeDataType(createDataType(INT32), new SerializableDataType());
    auto deserializedInt32 = DataTypeSerializationUtil::deserializeDataType(serializedInt32);
    ASSERT_TRUE(createDataType(INT32)->isEqual(deserializedInt32));

    // serialize and deserialize int64
    auto serializedInt64 = DataTypeSerializationUtil::serializeDataType(createDataType(INT64), new SerializableDataType());
    auto deserializedInt64 = DataTypeSerializationUtil::deserializeDataType(serializedInt64);
    ASSERT_TRUE(createDataType(INT64)->isEqual(deserializedInt64));

    // serialize and deserialize uint8
    auto serializedUInt8 = DataTypeSerializationUtil::serializeDataType(createDataType(UINT8), new SerializableDataType());
    auto deserializedUInt8 = DataTypeSerializationUtil::deserializeDataType(serializedUInt8);
    ASSERT_TRUE(createDataType(UINT8)->isEqual(deserializedUInt8));

    // serialize and deserialize uint16
    auto serializedUInt16 = DataTypeSerializationUtil::serializeDataType(createDataType(UINT16), new SerializableDataType());
    auto deserializedUInt16 = DataTypeSerializationUtil::deserializeDataType(serializedUInt16);
    ASSERT_TRUE(createDataType(UINT16)->isEqual(deserializedUInt16));

    // serialize and deserialize uint32
    auto serializedUInt32 = DataTypeSerializationUtil::serializeDataType(createDataType(UINT32), new SerializableDataType());
    auto deserializedUInt32 = DataTypeSerializationUtil::deserializeDataType(serializedUInt32);
    ASSERT_TRUE(createDataType(UINT32)->isEqual(deserializedUInt32));

    // serialize and deserialize uint64
    auto serializedUInt64 = DataTypeSerializationUtil::serializeDataType(createDataType(UINT64), new SerializableDataType());
    auto deserializedUInt64 = DataTypeSerializationUtil::deserializeDataType(serializedUInt64);
    ASSERT_TRUE(createDataType(UINT64)->isEqual(deserializedUInt64));

    // serialize and deserialize float32
    auto serializedFloat32 = DataTypeSerializationUtil::serializeDataType(createDataType(FLOAT32), new SerializableDataType());
    auto deserializedFloat32 = DataTypeSerializationUtil::deserializeDataType(serializedFloat32);
    ASSERT_TRUE(createDataType(FLOAT32)->isEqual(deserializedFloat32));

    // serialize and deserialize float64
    auto serializedFloat64 = DataTypeSerializationUtil::serializeDataType(createDataType(FLOAT64), new SerializableDataType());
    auto deserializedFloat64 = DataTypeSerializationUtil::deserializeDataType(serializedFloat64);
    ASSERT_TRUE(createDataType(FLOAT64)->isEqual(deserializedFloat64));

    // serialize and deserialize float64
    auto serializedArray = DataTypeSerializationUtil::serializeDataType(createArrayDataType(INT8, 42), new SerializableDataType());
    auto deserializedArray = DataTypeSerializationUtil::deserializeDataType(serializedArray);
    ASSERT_TRUE(createArrayDataType(INT8, 42)->isEqual(deserializedArray));

    /*
   std::string json_string;
   google::protobuf::util::MessageToJsonString(type, &json_string);
   std::cout << json_string << std::endl;
   */
}

TEST_F(SerializationUtilTest, schemaSerializationTest) {

    auto schema = Schema::create();
    schema->addField("f1", createDataType(FLOAT64));
    schema->addField("f2", createDataType(INT32));
    schema->addField("f3", createArrayDataType(INT8, 42));

    auto serializedSchema = SchemaSerializationUtil::serializeSchema(schema, new SerializableSchema());
    auto deserializedSchema = SchemaSerializationUtil::deserializeSchema(serializedSchema);
    ASSERT_TRUE(deserializedSchema->equals(schema));

    std::string json_string;
    auto options = google::protobuf::util::JsonOptions();
    options.add_whitespace = true;
    google::protobuf::util::MessageToJsonString(*serializedSchema, &json_string, options);
    std::cout << json_string << std::endl;
}

TEST_F(SerializationUtilTest, sourceDescriptorSerialization) {
    auto schema = Schema::create();
    schema->addField("f1", INT32);

    {
        auto source = ZmqSourceDescriptor::create(schema, "localhost", 42);
        auto serializedSourceDescriptor = OperatorSerializationUtil::serializeSourceSourceDescriptor(source, new SerializableOperator_SourceDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(serializedSourceDescriptor);
        ASSERT_TRUE(source->equal(deserializedSourceDescriptor));
    }

    {
        auto source = BinarySourceDescriptor::create(schema, "localhost");
        auto serializedSourceDescriptor = OperatorSerializationUtil::serializeSourceSourceDescriptor(source, new SerializableOperator_SourceDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(serializedSourceDescriptor);
        ASSERT_TRUE(source->equal(deserializedSourceDescriptor));
    }

    {
        auto source = CsvSourceDescriptor::create(schema, "localhost", ",", 10, 10);
        auto serializedSourceDescriptor = OperatorSerializationUtil::serializeSourceSourceDescriptor(source, new SerializableOperator_SourceDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(serializedSourceDescriptor);
        ASSERT_TRUE(source->equal(deserializedSourceDescriptor));
        std::string json_string;
        auto options = google::protobuf::util::JsonOptions();
        options.add_whitespace = true;
        google::protobuf::util::MessageToJsonString(*serializedSourceDescriptor, &json_string, options);
        std::cout << json_string << std::endl;
    }

    {
        auto source = DefaultSourceDescriptor::create(schema, 55, 42);
        auto serializedSourceDescriptor = OperatorSerializationUtil::serializeSourceSourceDescriptor(source, new SerializableOperator_SourceDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(serializedSourceDescriptor);
        ASSERT_TRUE(source->equal(deserializedSourceDescriptor));
    }

    {
        auto source = LogicalStreamSourceDescriptor::create("localhost");
        auto serializedSourceDescriptor = OperatorSerializationUtil::serializeSourceSourceDescriptor(source, new SerializableOperator_SourceDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(serializedSourceDescriptor);
        ASSERT_TRUE(source->equal(deserializedSourceDescriptor));
    }

    {
        auto source = SenseSourceDescriptor::create(schema, "senseusf");
        auto serializedSourceDescriptor = OperatorSerializationUtil::serializeSourceSourceDescriptor(source, new SerializableOperator_SourceDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(serializedSourceDescriptor);
        ASSERT_TRUE(source->equal(deserializedSourceDescriptor));
    }
}

TEST_F(SerializationUtilTest, sinkDescriptorSerialization) {

    {
        auto sink = ZmqSinkDescriptor::create("localhost", 42);
        auto serializedSinkDescriptor = OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        ASSERT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = PrintSinkDescriptor::create();
        auto serializedSinkDescriptor = OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        ASSERT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test", FILE_OVERWRITE, CSV_TYPE);
        auto serializedSinkDescriptor = OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        ASSERT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test", FILE_APPEND, BINARY_TYPE);
        auto serializedSinkDescriptor = OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        ASSERT_TRUE(sink->equal(deserializedSourceDescriptor));
    }
}

TEST_F(SerializationUtilTest, expressionSerialization) {

    {
        auto fieldAccess = FieldAccessExpressionNode::create(createDataType(INT32), "f1");
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(fieldAccess, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        ASSERT_TRUE(fieldAccess->equal(deserializedExpression));
    }
    auto f1 = FieldAccessExpressionNode::create(createDataType(INT32), "f1");
    auto f2 = FieldAccessExpressionNode::create(createDataType(INT32), "f2");

    {
        auto expression = AndExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        ASSERT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = OrExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        ASSERT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = EqualsExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        ASSERT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = LessExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        ASSERT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = LessEqualsExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        ASSERT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = GreaterExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        ASSERT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = GreaterExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        ASSERT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = AddExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        ASSERT_TRUE(expression->equal(deserializedExpression));
    }
    {
        auto expression = MulExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        ASSERT_TRUE(expression->equal(deserializedExpression));
    }
    {
        auto expression = DivExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        ASSERT_TRUE(expression->equal(deserializedExpression));
    }
    {
        auto expression = SubExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        ASSERT_TRUE(expression->equal(deserializedExpression));
    }
}

TEST_F(SerializationUtilTest, operatorSerialization) {

    auto source = createSourceLogicalOperatorNode(LogicalStreamSourceDescriptor::create("testStream"));
    auto filter = createFilterLogicalOperatorNode(Attribute("f1") == 10);
    filter->addChild(source);
    auto map = createMapLogicalOperatorNode(Attribute("f2") = 10);
    map->addChild(filter);
    auto sink = createSinkLogicalOperatorNode(PrintSinkDescriptor::create());
    sink->addChild(map);

    auto serializedOperator = OperatorSerializationUtil::serializeOperator(sink, new SerializableOperator());
    auto rootOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);

    ASSERT_TRUE(sink->equal(rootOperator));
    ASSERT_TRUE(sink->equalWithAllChildren(rootOperator));
}