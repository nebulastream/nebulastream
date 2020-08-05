#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <GRPC/Serialization/DataTypeSerializationUtil.hpp>
#include <GRPC/Serialization/ExpressionSerializationUtil.hpp>
#include <GRPC/Serialization/OperatorSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <SerializableOperator.pb.h>
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
    auto serializedInt8 = DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createInt8(), new SerializableDataType());
    auto deserializedInt8 = DataTypeSerializationUtil::deserializeDataType(serializedInt8);
    ASSERT_TRUE(DataTypeFactory::createInt8()->isEquals(deserializedInt8));

    // serialize and deserialize int16
    auto serializedInt16 = DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createInt16(), new SerializableDataType());
    auto deserializedInt16 = DataTypeSerializationUtil::deserializeDataType(serializedInt16);
    ASSERT_TRUE(DataTypeFactory::createInt16()->isEquals(deserializedInt16));

    // serialize and deserialize int32
    auto serializedInt32 = DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createInt32(), new SerializableDataType());
    auto deserializedInt32 = DataTypeSerializationUtil::deserializeDataType(serializedInt32);
    ASSERT_TRUE(DataTypeFactory::createInt32()->isEquals(deserializedInt32));

    // serialize and deserialize int64
    auto serializedInt64 = DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createInt64(), new SerializableDataType());
    auto deserializedInt64 = DataTypeSerializationUtil::deserializeDataType(serializedInt64);
    ASSERT_TRUE(DataTypeFactory::createInt64()->isEquals(deserializedInt64));

    // serialize and deserialize uint8
    auto serializedUInt8 = DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createUInt8(), new SerializableDataType());
    auto deserializedUInt8 = DataTypeSerializationUtil::deserializeDataType(serializedUInt8);
    ASSERT_TRUE(DataTypeFactory::createUInt8()->isEquals(deserializedUInt8));

    // serialize and deserialize uint16
    auto serializedUInt16 = DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createUInt16(), new SerializableDataType());
    auto deserializedUInt16 = DataTypeSerializationUtil::deserializeDataType(serializedUInt16);
    ASSERT_TRUE(DataTypeFactory::createUInt16()->isEquals(deserializedUInt16));

    // serialize and deserialize uint32
    auto serializedUInt32 = DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createUInt32(), new SerializableDataType());
    auto deserializedUInt32 = DataTypeSerializationUtil::deserializeDataType(serializedUInt32);
    ASSERT_TRUE(DataTypeFactory::createUInt32()->isEquals(deserializedUInt32));

    // serialize and deserialize uint64
    auto serializedUInt64 = DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createUInt64(), new SerializableDataType());
    auto deserializedUInt64 = DataTypeSerializationUtil::deserializeDataType(serializedUInt64);
    ASSERT_TRUE(DataTypeFactory::createUInt64()->isEquals(deserializedUInt64));

    // serialize and deserialize float32
    auto serializedFloat32 = DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createFloat(), new SerializableDataType());
    auto deserializedFloat32 = DataTypeSerializationUtil::deserializeDataType(serializedFloat32);
    ASSERT_TRUE(DataTypeFactory::createFloat()->isEquals(deserializedFloat32));

    // serialize and deserialize float64
    auto serializedFloat64 = DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createDouble(), new SerializableDataType());
    auto deserializedFloat64 = DataTypeSerializationUtil::deserializeDataType(serializedFloat64);
    ASSERT_TRUE(DataTypeFactory::createDouble()->isEquals(deserializedFloat64));

    // serialize and deserialize float64
    auto serializedArray = DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createArray(42, DataTypeFactory::createInt8()), new SerializableDataType());
    auto deserializedArray = DataTypeSerializationUtil::deserializeDataType(serializedArray);
    ASSERT_TRUE(DataTypeFactory::createArray(42, DataTypeFactory::createInt8())->isEquals(deserializedArray));

    /*
   std::string json_string;
   google::protobuf::util::MessageToJsonString(type, &json_string);
   std::cout << json_string << std::endl;
   */
}

TEST_F(SerializationUtilTest, schemaSerializationTest) {

    auto schema = Schema::create();
    schema->addField("f1", DataTypeFactory::createDouble());
    schema->addField("f2", DataTypeFactory::createInt32());
    schema->addField("f3", DataTypeFactory::createArray(42, DataTypeFactory::createInt8()));

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

    {
        Network::NesPartition nesPartition{1, 22, 33, 44};
        auto source = Network::NetworkSourceDescriptor::create(schema, nesPartition);
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
        auto sink = FileSinkDescriptor::create("test");
        auto serializedSinkDescriptor = OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        ASSERT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test", "TEXT_FORMAT", "APPEND");
        auto serializedSinkDescriptor = OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        ASSERT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test", "TEXT_FORMAT", "OVERWRITE");
        auto serializedSinkDescriptor = OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        ASSERT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test", "NES_FORMAT", "OVERWRITE");
        auto serializedSinkDescriptor = OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        ASSERT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test", "CSV_FORMAT", "OVERWRITE");
        auto serializedSinkDescriptor = OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        ASSERT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        Network::NodeLocation nodeLocation{1, "localhost", 31337};
        Network::NesPartition nesPartition{1, 22, 33, 44};
        auto sink = Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, std::chrono::seconds(1), 5);
        auto serializedSinkDescriptor = OperatorSerializationUtil::serializeSinkDescriptor(sink, new SerializableOperator_SinkDetails());
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(serializedSinkDescriptor);
        ASSERT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

}

TEST_F(SerializationUtilTest, expressionSerialization) {

    {
        auto fieldAccess = FieldAccessExpressionNode::create(DataTypeFactory::createInt32(), "f1");
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(fieldAccess, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        ASSERT_TRUE(fieldAccess->equal(deserializedExpression));
    }
    auto f1 = FieldAccessExpressionNode::create(DataTypeFactory::createInt32(), "f1");
    auto f2 = FieldAccessExpressionNode::create(DataTypeFactory::createInt32(), "f2");

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