/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <API/AttributeField.hpp>
#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Query.hpp>
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <API/Windowing.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Expressions/ArithmeticalExpressions/AbsExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/SqrtExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Expressions/CaseExpressionNode.hpp>
#include <Expressions/ExpressionSerializationUtil.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Expressions/Functions/FunctionExpressionNode.hpp>
#include <Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Expressions/WhenExpressionNode.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalIntervalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/LogicalIntervalJoinOperator.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/OPCSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/StatisticSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/CountMinDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/IngestionRate.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyASAP.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyAdaptive.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyLazy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/NeverTrigger.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/Serialization/OperatorSerializationUtil.hpp>
#include <Operators/Serialization/QueryPlanSerializationUtil.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableQueryPlan.pb.h>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Types/ThresholdWindow.hpp>
#include <Util/JavaUDFDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <iostream>

using namespace NES;
using namespace Configurations;
static constexpr auto NSOURCE_RETRIES = 100;
static constexpr auto NSOURCE_RETRY_WAIT = std::chrono::milliseconds(5);
class SerializationUtilTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SerializationUtilTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SerializationUtilTest test class.");
    }
};

TEST_F(SerializationUtilTest, dataTypeSerialization) {

    // serialize and deserialize int8
    auto serializedInt8 = DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createInt8(), new SerializableDataType());
    auto deserializedInt8 = DataTypeSerializationUtil::deserializeDataType(*serializedInt8);
    EXPECT_TRUE(DataTypeFactory::createInt8()->equals(deserializedInt8));

    // serialize and deserialize int16
    auto serializedInt16 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createInt16(), new SerializableDataType());
    auto deserializedInt16 = DataTypeSerializationUtil::deserializeDataType(*serializedInt16);
    EXPECT_TRUE(DataTypeFactory::createInt16()->equals(deserializedInt16));

    // serialize and deserialize int32
    auto serializedInt32 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createInt32(), new SerializableDataType());
    auto deserializedInt32 = DataTypeSerializationUtil::deserializeDataType(*serializedInt32);
    EXPECT_TRUE(DataTypeFactory::createInt32()->equals(deserializedInt32));

    // serialize and deserialize int64
    auto serializedInt64 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createInt64(), new SerializableDataType());
    auto deserializedInt64 = DataTypeSerializationUtil::deserializeDataType(*serializedInt64);
    EXPECT_TRUE(DataTypeFactory::createInt64()->equals(deserializedInt64));

    // serialize and deserialize uint8
    auto serializedUInt8 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createUInt8(), new SerializableDataType());
    auto deserializedUInt8 = DataTypeSerializationUtil::deserializeDataType(*serializedUInt8);
    EXPECT_TRUE(DataTypeFactory::createUInt8()->equals(deserializedUInt8));

    // serialize and deserialize uint16
    auto serializedUInt16 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createUInt16(), new SerializableDataType());
    auto deserializedUInt16 = DataTypeSerializationUtil::deserializeDataType(*serializedUInt16);
    EXPECT_TRUE(DataTypeFactory::createUInt16()->equals(deserializedUInt16));

    // serialize and deserialize uint32
    auto serializedUInt32 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createUInt32(), new SerializableDataType());
    auto deserializedUInt32 = DataTypeSerializationUtil::deserializeDataType(*serializedUInt32);
    EXPECT_TRUE(DataTypeFactory::createUInt32()->equals(deserializedUInt32));

    // serialize and deserialize uint64
    auto serializedUInt64 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createUInt64(), new SerializableDataType());
    auto deserializedUInt64 = DataTypeSerializationUtil::deserializeDataType(*serializedUInt64);
    EXPECT_TRUE(DataTypeFactory::createUInt64()->equals(deserializedUInt64));

    // serialize and deserialize float32
    auto serializedFloat32 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createFloat(), new SerializableDataType());
    auto deserializedFloat32 = DataTypeSerializationUtil::deserializeDataType(*serializedFloat32);
    EXPECT_TRUE(DataTypeFactory::createFloat()->equals(deserializedFloat32));

    // serialize and deserialize float64
    auto serializedFloat64 =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createDouble(), new SerializableDataType());
    auto deserializedFloat64 = DataTypeSerializationUtil::deserializeDataType(*serializedFloat64);
    EXPECT_TRUE(DataTypeFactory::createDouble()->equals(deserializedFloat64));

    // serialize and deserialize array
    auto serializedArray =
        DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createArray(42, DataTypeFactory::createInt8()),
                                                     new SerializableDataType());
    auto deserializedArray = DataTypeSerializationUtil::deserializeDataType(*serializedArray);
    EXPECT_TRUE(DataTypeFactory::createArray(42, DataTypeFactory::createInt8())->equals(deserializedArray));

    // serialize and deserialize array
    auto serializedText = DataTypeSerializationUtil::serializeDataType(DataTypeFactory::createText(), new SerializableDataType());
    auto deserializedText = DataTypeSerializationUtil::deserializeDataType(*serializedText);
    EXPECT_TRUE(DataTypeFactory::createText()->equals(deserializedText));
}

TEST_F(SerializationUtilTest, schemaSerializationTest) {

    auto schema = Schema::create();
    schema->addField("f1", DataTypeFactory::createDouble());
    schema->addField("f2", DataTypeFactory::createInt32());
    schema->addField("f3", DataTypeFactory::createArray(42, DataTypeFactory::createInt8()));
    schema->addField("f4", DataTypeFactory::createText());

    auto serializedSchema = SchemaSerializationUtil::serializeSchema(schema, new SerializableSchema());
    auto deserializedSchema = SchemaSerializationUtil::deserializeSchema(*serializedSchema);
    EXPECT_TRUE(deserializedSchema->equals(schema));
}

TEST_F(SerializationUtilTest, schemaSerializationTestColumnLayout) {

    auto schema = Schema::create();
    schema->addField("f1", DataTypeFactory::createDouble());
    schema->addField("f2", DataTypeFactory::createInt32());
    schema->addField("f3", DataTypeFactory::createArray(42, DataTypeFactory::createInt8()));
    schema->addField("f4", DataTypeFactory::createText());
    schema->setLayoutType(NES::Schema::MemoryLayoutType::COLUMNAR_LAYOUT);

    auto serializedSchema = SchemaSerializationUtil::serializeSchema(schema, new SerializableSchema());
    auto deserializedSchema = SchemaSerializationUtil::deserializeSchema(*serializedSchema);
    EXPECT_TRUE(deserializedSchema->equals(schema));
    EXPECT_EQ(deserializedSchema->getLayoutType(), NES::Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
}

TEST_F(SerializationUtilTest, sourceDescriptorSerialization) {
    auto schema = Schema::create();
    schema->addField("f1", BasicType::INT32);

    {
        auto source = ZmqSourceDescriptor::create(schema, "localhost", 42);
        SerializableOperator_SourceDetails sourceDetails;
        OperatorSerializationUtil::serializeSourceDescriptor(*source, sourceDetails);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(sourceDetails);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
    }

#if ENABLE_OPC_BUILD
    {
        UA_NodeId nodeId = UA_NODEID_STRING(1, (char*) "the.answer");
        auto source = OPCSourceDescriptor::create(schema, "localhost", nodeId, "", "");
        SerializableOperator_SourceDetails sourceDetails;
        OperatorSerializationUtil::serializeSourceDescriptor(*source, sourceDetails);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(sourceDetails);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
    }
#endif

    {
        auto source = BinarySourceDescriptor::create(schema, "localhost");
        SerializableOperator_SourceDetails sourceDetails;
        OperatorSerializationUtil::serializeSourceDescriptor(*source, sourceDetails);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(sourceDetails);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
    }

    {
        auto csvSourceType = CSVSourceType::create("testStream", "physical_test");

        csvSourceType->setFilePath("localhost");
        csvSourceType->setNumberOfBuffersToProduce(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(10);
        csvSourceType->setGatheringInterval(10);
        auto source = CsvSourceDescriptor::create(schema, csvSourceType);
        SerializableOperator_SourceDetails sourceDetails;
        OperatorSerializationUtil::serializeSourceDescriptor(*source, sourceDetails);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(sourceDetails);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
        std::string json_string;
        auto options = google::protobuf::util::JsonOptions();
        options.add_whitespace = true;
        google::protobuf::util::MessageToJsonString(sourceDetails, &json_string, options);
        NES_DEBUG("{}", json_string);
    }

    {
        auto source = DefaultSourceDescriptor::create(schema, 55, 42);
        SerializableOperator_SourceDetails sourceDetails;
        OperatorSerializationUtil::serializeSourceDescriptor(*source, sourceDetails);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(sourceDetails);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
    }

    {
        auto source = LogicalSourceDescriptor::create("localhost");
        SerializableOperator_SourceDetails sourceDetails;
        OperatorSerializationUtil::serializeSourceDescriptor(*source, sourceDetails);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(sourceDetails);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
    }

    {
        auto source = SenseSourceDescriptor::create(schema, "senseusf");
        SerializableOperator_SourceDetails sourceDetails;
        OperatorSerializationUtil::serializeSourceDescriptor(*source, sourceDetails);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(sourceDetails);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
    }

    {
        Network::NodeLocation nodeLocation{WorkerId(0), "*", 31337};
        Network::NesPartition nesPartition{SharedQueryId(1), OperatorId(22), PartitionId(33), SubpartitionId(44)};
        uint16_t version = 55;
        auto uniqueId = 66;
        auto source = Network::NetworkSourceDescriptor::create(schema,
                                                               nesPartition,
                                                               nodeLocation,
                                                               NSOURCE_RETRY_WAIT,
                                                               NSOURCE_RETRIES,
                                                               version,
                                                               OperatorId(uniqueId));
        SerializableOperator_SourceDetails sourceDetails;
        OperatorSerializationUtil::serializeSourceDescriptor(*source, sourceDetails);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(sourceDetails);
        EXPECT_TRUE(source->equal(deserializedSourceDescriptor));
    }

    {
        auto tcpSourceConfig = TCPSourceType::create("testStream", "physical_test");
        auto tcpSource = TCPSourceDescriptor::create(schema, tcpSourceConfig);
        SerializableOperator_SourceDetails sourceDetails;
        OperatorSerializationUtil::serializeSourceDescriptor(*tcpSource, sourceDetails);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSourceDescriptor(sourceDetails);
        EXPECT_TRUE(tcpSource->equal(deserializedSourceDescriptor));
    }
}

TEST_F(SerializationUtilTest, sinkDescriptorSerialization) {

    {
        auto sink = ZmqSinkDescriptor::create("localhost", 42);
        SerializableOperator_SinkDetails sinkDetails;
        OperatorSerializationUtil::serializeSinkDescriptor(*sink, sinkDetails, 0);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(sinkDetails);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

#if ENABLE_OPC_BUILD
    {
        UA_NodeId nodeId = UA_NODEID_STRING(1, (char*) "the.answer");
        auto sink = OPCSinkDescriptor::create("localhost", nodeId, "", "");
        SerializableOperator_SinkDetails sinkDescriptor;
        OperatorSerializationUtil::serializeSinkDescriptor(*sink, sinkDescriptor, 0);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(sinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }
#endif

#ifdef ENABLE_KAFKA_BUILD
    {
        auto sink = KafkaSinkDescriptor::create("CSV_FORMAT", "test-topic", "localhost:9092", 1234);
        SerializableOperator_SinkDetails sinkDetails;
        OperatorSerializationUtil::serializeSinkDescriptor(*sink, sinkDetails, 0);
        auto deserializedSinkDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(sinkDetails);
        EXPECT_TRUE(sink->equal(deserializedSinkDescriptor));
    };
#endif

    {
        auto sink = PrintSinkDescriptor::create();
        SerializableOperator_SinkDetails sinkDescriptor;
        OperatorSerializationUtil::serializeSinkDescriptor(*sink, sinkDescriptor, 0);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(sinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test");
        SerializableOperator_SinkDetails sinkDescriptor;
        OperatorSerializationUtil::serializeSinkDescriptor(*sink, sinkDescriptor, 0);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(sinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test", "TEXT_FORMAT", "APPEND");
        SerializableOperator_SinkDetails sinkDescriptor;
        OperatorSerializationUtil::serializeSinkDescriptor(*sink, sinkDescriptor, 0);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(sinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test", "TEXT_FORMAT", "OVERWRITE");
        SerializableOperator_SinkDetails sinkDescriptor;
        OperatorSerializationUtil::serializeSinkDescriptor(*sink, sinkDescriptor, 0);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(sinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test", "NES_FORMAT", "OVERWRITE");
        SerializableOperator_SinkDetails sinkDescriptor;
        OperatorSerializationUtil::serializeSinkDescriptor(*sink, sinkDescriptor, 0);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(sinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        auto sink = FileSinkDescriptor::create("test", "CSV_FORMAT", "OVERWRITE");
        SerializableOperator_SinkDetails sinkDescriptor;
        OperatorSerializationUtil::serializeSinkDescriptor(*sink, sinkDescriptor, 0);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(sinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        Network::NodeLocation nodeLocation{WorkerId(1), "localhost", 31337};
        Network::NesPartition nesPartition{SharedQueryId(1), OperatorId(22), PartitionId(33), SubpartitionId(44)};
        auto retryTimes = 8;
        DecomposedQueryPlanVersion version = 5;
        auto numberOfOrigins = 6;
        OperatorId uniqueId = OperatorId(7);
        auto sink = Network::NetworkSinkDescriptor::create(nodeLocation,
                                                           nesPartition,
                                                           std::chrono::seconds(1),
                                                           retryTimes,
                                                           version,
                                                           numberOfOrigins,
                                                           uniqueId, INVALID_OPERATOR_ID);
        SerializableOperator_SinkDetails sinkDescriptor;
        OperatorSerializationUtil::serializeSinkDescriptor(*sink, sinkDescriptor, 0);
        auto deserializedSourceDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(sinkDescriptor);
        EXPECT_TRUE(sink->equal(deserializedSourceDescriptor));
    }

    {
        // Testing for all StatisticSinkFormatType, if we can serialize a statistic sink
        constexpr auto numberOfOrigins = 42;// just some arbitrary number
        for (auto sinkFormatType : magic_enum::enum_values<Statistic::StatisticSynopsisType>()) {
            for (auto sinkDataCodec : magic_enum::enum_values<Statistic::StatisticDataCodec>()) {
                auto sink = Statistic::StatisticSinkDescriptor::create(sinkFormatType, sinkDataCodec, numberOfOrigins);
                SerializableOperator_SinkDetails sinkDescriptor;
                OperatorSerializationUtil::serializeSinkDescriptor(*sink, sinkDescriptor, numberOfOrigins);
                auto deserializedSinkDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(sinkDescriptor);
                EXPECT_TRUE(sink->equal(deserializedSinkDescriptor));
            }
        }
    }
}

TEST_F(SerializationUtilTest, expressionSerialization) {

    {
        auto fieldAccess = FieldAccessExpressionNode::create(DataTypeFactory::createInt32(), "f1");
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(fieldAccess, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(fieldAccess->equal(deserializedExpression));
    }
    auto f1 = FieldAccessExpressionNode::create(DataTypeFactory::createInt32(), "f1");
    auto f2 = FieldAccessExpressionNode::create(DataTypeFactory::createInt32(), "f2");

    {
        auto expression = AndExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = OrExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = EqualsExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = LessExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = LessEqualsExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = GreaterExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = GreaterExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }

    {
        auto expression = AddExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }
    {
        auto expression = MulExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }
    {
        auto expression = DivExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }
    {
        auto expression = SubExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }
    {
        auto expression = AbsExpressionNode::create(f1);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }
    {
        auto expression = SqrtExpressionNode::create(f1);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }
    {
        auto expression = CaseExpressionNode::create({f1, f2}, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }
    {
        auto expression = WhenExpressionNode::create(f1, f2);
        auto serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
        auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
        EXPECT_TRUE(expression->equal(deserializedExpression));
    }
}

TEST_F(SerializationUtilTest, functionExpressionSerialization) {
    auto argument1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), "1"));
    auto expression = FunctionExpression::create(argument1->getStamp(), "ln", {argument1});
    auto* serializedExpression = ExpressionSerializationUtil::serializeExpression(expression, new SerializableExpression());
    auto deserializedExpression = ExpressionSerializationUtil::deserializeExpression(*serializedExpression);
    EXPECT_TRUE(expression->equal(deserializedExpression));
}

TEST_F(SerializationUtilTest, operatorSerialization) {

    {
        auto javaUDFDescriptor = NES::Catalogs::UDF::JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
        auto javaUDFMap = LogicalOperatorFactory::createMapUDFLogicalOperator(javaUDFDescriptor);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(javaUDFMap);
        auto deserializedOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(javaUDFMap->equal(deserializedOperator));
    }

    {
        auto javaUDFDescriptor = NES::Catalogs::UDF::JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
        auto javaUDFMap = LogicalOperatorFactory::createFlatMapUDFLogicalOperator(javaUDFDescriptor);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(javaUDFMap);
        auto deserializedOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(javaUDFMap->equal(deserializedOperator));
    }

    {
        auto rename = LogicalOperatorFactory::createRenameSourceOperator("newSourceName");
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(rename);
        auto renameOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(rename->equal(renameOperator));
    }

    {
        auto source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("testStream"));
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(source);
        auto sourceOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(source->equal(sourceOperator));
    }

#ifdef TFDEF
    {
        auto f1_in = std::make_shared<ExpressionItem>(Attribute("f1", NES::BasicType::FLOAT32));
        auto f2_in = std::make_shared<ExpressionItem>(Attribute("f2", NES::BasicType::FLOAT32));
        auto f1_out = std::make_shared<ExpressionItem>(Attribute("f1_out", NES::BasicType::FLOAT32));
        auto f2_out = std::make_shared<ExpressionItem>(Attribute("f2_out", NES::BasicType::FLOAT32));
        auto f3_out = std::make_shared<ExpressionItem>(Attribute("f3_out", NES::BasicType::FLOAT32));

        auto inferModel = LogicalOperatorFactory::createInferModelOperator(
            "test.file",
            {f1_in->getExpressionNode(), f2_in->getExpressionNode()},
            {f1_out->getExpressionNode(), f2_out->getExpressionNode(), f3_out->getExpressionNode()});
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(inferModel);
        auto inferModelOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(inferModel->equal(inferModelOperator));
    }
#endif// TFDEF

    {
        auto filter = LogicalOperatorFactory::createFilterOperator(Attribute("f1") == 10);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(filter);
        auto filterOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(filter->equal(filterOperator));
    }

    {
        auto map = LogicalOperatorFactory::createMapOperator(Attribute("f2") = 10);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(map);
        auto mapOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(map->equal(mapOperator));
    }

    {
        auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3") = MOD(2, Attribute("f3")));
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(map);
        auto mapOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(map->equal(mapOperator));
    }

    {
        auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3") = POWER(2, Attribute("f3")));
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(map);
        auto mapOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(map->equal(mapOperator));
    }

    {
        auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3") = ABS(Attribute("f3")));
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(map);
        auto mapOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(map->equal(mapOperator));
    }
    {
        auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(sink);
        auto sinkOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(sink->equal(sinkOperator));
    }

    {
        auto unionOp = LogicalOperatorFactory::createUnionOperator();
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(unionOp);
        auto unionOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(unionOp->equal(unionOperator));
    }

    {
        auto joinExpression = EqualsExpressionNode::create(
            FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
            FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>());

        Join::LogicalJoinDescriptorPtr joinDef = Join::LogicalJoinDescriptor::create(
            joinExpression,
            Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(), API::Milliseconds(10)),
            1,
            1,
            NES::Join::LogicalJoinDescriptor::JoinType::INNER_JOIN);

        auto join = LogicalOperatorFactory::createJoinOperator(joinDef)->as<LogicalJoinOperator>();
        join->setOriginId(OriginId(42));
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(join);
        auto joinOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(join->equal(joinOperator));
    }
    //testing the interval join
    {
        NES_DEBUG("testing the interval join")
        Join::LogicalIntervalJoinDescriptorPtr joinDef =
            Join::LogicalIntervalJoinDescriptor::create((Attribute("key") == Attribute("key")),
                                                        EventTime(Attribute("timeStamp")),
                                                        4,
                                                        5);
        auto join = LogicalOperatorFactory::createIntervalJoinOperator(joinDef)->as<LogicalIntervalJoinOperator>();
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(join);
        auto deserializedOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(join->equal(deserializedOperator));
    }
    // cross join test
    {
        auto joinExpression = ExpressionItem(true).getExpressionNode();

        Join::LogicalJoinDescriptorPtr joinDef = Join::LogicalJoinDescriptor::create(
            joinExpression,
            Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(), API::Milliseconds(10)),
            1,
            1,
            NES::Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT);

        auto join = LogicalOperatorFactory::createJoinOperator(joinDef)->as<LogicalJoinOperator>();
        join->setOriginId(OriginId(42));
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(join);
        auto joinOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        join->setOriginId(OriginId(42));
        EXPECT_TRUE(join->equal(joinOperator));
    }
    {
        auto windowType = Windowing::TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10));
        auto timeBasedWindowType = windowType->as<Windowing::TimeBasedWindowType>();
        auto watermarkAssignerDescriptor = Windowing::EventTimeWatermarkStrategyDescriptor::create(
            Attribute(timeBasedWindowType->getTimeCharacteristic()->getField()->getName()).getExpressionNode(),
            API::Milliseconds(0),
            timeBasedWindowType->getTimeCharacteristic()->getTimeUnit());

        auto watermarkAssignerOperator = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkAssignerDescriptor);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(watermarkAssignerOperator);
        auto deserializedOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(watermarkAssignerOperator->equal(deserializedOperator));
    }

    {
        auto watermarkAssignerDescriptor = Windowing::IngestionTimeWatermarkStrategyDescriptor::create();
        auto watermarkAssignerOperator = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkAssignerDescriptor);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(watermarkAssignerOperator);
        auto deserializedOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(watermarkAssignerOperator->equal(deserializedOperator));
    }

    {
        auto windowType = Windowing::TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10));
        auto windowDefinition =
            Windowing::LogicalWindowDescriptor::create({API::Sum(Attribute("test"))->aggregation}, windowType, 0);
        auto tumblingWindow = LogicalOperatorFactory::createWindowOperator(windowDefinition);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(tumblingWindow);
        auto deserializedOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(tumblingWindow->equal(deserializedOperator));
    }

    {
        auto windowType = Windowing::SlidingWindow::of(EventTime(Attribute("ts")), Seconds(10), Hours(200));
        auto windowDefinition =
            Windowing::LogicalWindowDescriptor::create({API::Sum(Attribute("test"))->aggregation}, windowType, 0);
        auto slidingWindow = LogicalOperatorFactory::createWindowOperator(windowDefinition);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(slidingWindow);
        auto deserializedOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(slidingWindow->equal(deserializedOperator));
    }

    // threshold window operator
    {
        auto windowType = Windowing::ThresholdWindow::of(Attribute("f1") < 45);
        auto windowDefinition =
            Windowing::LogicalWindowDescriptor::create({API::Sum(Attribute("test"))->aggregation}, windowType, 0);
        auto thresholdWindow = LogicalOperatorFactory::createWindowOperator(windowDefinition);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(thresholdWindow);
        auto deserializedOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(thresholdWindow->equal(deserializedOperator));
    }

    // threshold window operator with minimum count
    {
        auto windowType = Windowing::ThresholdWindow::of(Attribute("f1") < 45, 5);
        auto windowDefinition =
            Windowing::LogicalWindowDescriptor::create({API::Sum(Attribute("test"))->aggregation}, windowType, 0);
        auto thresholdWindow = LogicalOperatorFactory::createWindowOperator(windowDefinition);
        auto serializedOperator = OperatorSerializationUtil::serializeOperator(thresholdWindow);
        auto deserializedOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
        EXPECT_TRUE(thresholdWindow->equal(deserializedOperator));
    }

    {
        // Testing for all possible combinations of sending policies and trigger conditions, if we can serialize a
        // statistic build operator correctly
        auto allPossibleSendingPolicies = {Statistic::SENDING_ASAP(Statistic::StatisticDataCodec::DEFAULT),
                                           Statistic::SENDING_ADAPTIVE(Statistic::StatisticDataCodec::DEFAULT),
                                           Statistic::SENDING_LAZY(Statistic::StatisticDataCodec::DEFAULT)};
        auto allPossibleTriggerCondition = {Statistic::NeverTrigger::create()};
        for (auto sendingPolicy : allPossibleSendingPolicies) {
            for (auto triggerCondition : allPossibleTriggerCondition) {
                auto metric = Statistic::IngestionRate::create();
                auto statisticDescriptor = Statistic::CountMinDescriptor::create(metric->getField());
                auto windowType = Windowing::TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10));
                auto statisticBuildOperator = LogicalOperatorFactory::createStatisticBuildOperator(windowType,
                                                                                                   statisticDescriptor,
                                                                                                   metric->hash(),
                                                                                                   sendingPolicy,
                                                                                                   triggerCondition);
                statisticBuildOperator->setStatisticId(getNextStatisticId());
                auto serializedOperator = OperatorSerializationUtil::serializeOperator(statisticBuildOperator);
                auto deserializedOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);
                EXPECT_TRUE(statisticBuildOperator->equal(deserializedOperator));
            }
        }
    }
}

TEST_F(SerializationUtilTest, queryPlanSerDeSerialization) {

    auto source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("testStream"));
    auto filter = LogicalOperatorFactory::createFilterOperator(Attribute("f1") == 10);
    filter->addChild(source);
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f2") = 10);
    map->addChild(filter);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    sink->addChild(map);

    auto queryPlan = QueryPlan::create(QueryId(1), {sink});

    auto serializedQueryPlan = new SerializableQueryPlan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan);
    auto deserializedQueryPlan = QueryPlanSerializationUtil::deserializeQueryPlan(serializedQueryPlan);

    EXPECT_TRUE(deserializedQueryPlan->getQueryId() == queryPlan->getQueryId());
    EXPECT_TRUE(deserializedQueryPlan->getRootOperators()[0]->equal(queryPlan->getRootOperators()[0]));
}

TEST_F(SerializationUtilTest, queryPlanSerDeSerializationMultipleFilters) {
    auto source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("testStream"));
    auto filter1 = LogicalOperatorFactory::createFilterOperator(Attribute("f1") == 10);
    auto filter2 = LogicalOperatorFactory::createFilterOperator(Attribute("f2") == 20);
    auto filter3 = LogicalOperatorFactory::createFilterOperator(Attribute("f3") == 30);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());

    filter1->addChild(source);
    filter2->addChild(filter1);
    filter3->addChild(filter2);
    sink->addChild(filter3);

    auto queryPlan = QueryPlan::create(QueryId(1), {sink});

    auto serializedQueryPlan = new SerializableQueryPlan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan);
    auto deserializedQueryPlan = QueryPlanSerializationUtil::deserializeQueryPlan(serializedQueryPlan);

    EXPECT_TRUE(deserializedQueryPlan->getQueryId() == queryPlan->getQueryId());
    EXPECT_TRUE(deserializedQueryPlan->getRootOperators()[0]->equal(queryPlan->getRootOperators()[0]));
}

TEST_F(SerializationUtilTest, queryPlanSerDeSerializationColumnarLayout) {
    auto source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("testStream"));
    auto filter = LogicalOperatorFactory::createFilterOperator(Attribute("f1") == 10);
    filter->addChild(source);
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f2") = 10);
    map->addChild(filter);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    sink->addChild(map);

    auto queryPlan = QueryPlan::create(QueryId(1), {sink});

    auto serializedQueryPlan = new SerializableQueryPlan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan);
    auto deserializedQueryPlan = QueryPlanSerializationUtil::deserializeQueryPlan(serializedQueryPlan);

    EXPECT_TRUE(deserializedQueryPlan->getQueryId() == queryPlan->getQueryId());
    EXPECT_TRUE(deserializedQueryPlan->getRootOperators()[0]->equal(queryPlan->getRootOperators()[0]));
}

#if ENABLE_OPC_BUILD
TEST_F(SerializationUtilTest, queryPlanWithOPCSerDeSerialization) {

    auto schema = Schema::create();
    schema->addField("f1", BasicType::INT32);
    UA_NodeId nodeId = UA_NODEID_STRING(1, (char*) "the.answer");
    auto source = LogicalOperatorFactory::createSourceOperator(OPCSourceDescriptor::create(schema, "localhost", nodeId, "", ""));
    auto filter = LogicalOperatorFactory::createFilterOperator(Attribute("f1") == 10);
    filter->addChild(source);
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f2") = 10);
    map->addChild(filter);
    auto sink = LogicalOperatorFactory::createSinkOperator(OPCSinkDescriptor::create("localhost", nodeId, "", ""));
    sink->addChild(map);

    auto queryPlan = QueryPlan::create(1, 1, {sink});

    auto serializedQueryPlan = new SerializableQueryPlan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan);
    auto deserializedQueryPlan = QueryPlanSerializationUtil::deserializeQueryPlan(serializedQueryPlan);

    EXPECT_TRUE(deserializedQueryPlan->getQueryId() == queryPlan->getQueryId());
    EXPECT_TRUE(deserializedQueryPlan->getQuerySubPlanId() == queryPlan->getQuerySubPlanId());
    EXPECT_TRUE(deserializedQueryPlan->getRootOperators()[0]->equal(queryPlan->getRootOperators()[0]));
}
#endif

TEST_F(SerializationUtilTest, queryPlanWithMultipleRootSerDeSerialization) {

    auto source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("testStream"));
    auto filter = LogicalOperatorFactory::createFilterOperator(Attribute("f1") == 10);
    filter->addChild(source);
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f2") = 10);
    map->addChild(filter);
    auto sink1 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    auto sink2 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    sink1->addChild(map);
    sink2->addChild(map);

    auto queryPlan = QueryPlan::create(QueryId(1), {sink1, sink2});

    auto serializedQueryPlan = new SerializableQueryPlan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan);
    auto deserializedQueryPlan = QueryPlanSerializationUtil::deserializeQueryPlan(serializedQueryPlan);

    EXPECT_TRUE(deserializedQueryPlan->getQueryId() == queryPlan->getQueryId());

    std::vector<OperatorPtr> actualRootOperators = deserializedQueryPlan->getRootOperators();
    for (const auto& actualRootOperator : actualRootOperators) {
        bool found = false;
        for (const auto& queryRoot : queryPlan->getRootOperators()) {
            if (actualRootOperator->equal(queryRoot)) {
                EXPECT_TRUE(actualRootOperator->equalWithAllChildren(queryRoot));
                found = true;
            }
        }
        EXPECT_TRUE(found);
    }
}

TEST_F(SerializationUtilTest, queryPlanWithMultipleSourceSerDeSerialization) {
    auto source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("testStream"));
    auto source2 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("testStream"));
    auto filter = LogicalOperatorFactory::createFilterOperator(Attribute("f1") == 10);
    filter->addChild(source1);
    filter->addChild(source2);
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f2") = 10);
    map->addChild(filter);
    auto sink1 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    auto sink2 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    sink1->addChild(map);
    sink2->addChild(map);

    auto queryPlan = QueryPlan::create(QueryId(1), {sink1, sink2});

    auto serializedQueryPlan = new SerializableQueryPlan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan);
    auto deserializedQueryPlan = QueryPlanSerializationUtil::deserializeQueryPlan(serializedQueryPlan);

    EXPECT_TRUE(deserializedQueryPlan->getQueryId() == queryPlan->getQueryId());

    std::vector<OperatorPtr> actualRootOperators = deserializedQueryPlan->getRootOperators();
    for (const auto& actualRootOperator : actualRootOperators) {
        bool found = false;
        for (const auto& queryRoot : queryPlan->getRootOperators()) {
            if (actualRootOperator->equal(queryRoot)) {
                EXPECT_TRUE(actualRootOperator->equalWithAllChildren(queryRoot));
                found = true;
            }
        }
        EXPECT_TRUE(found);
    }
    EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->as<Operator>()->getId()
                == actualRootOperators[1]->getChildren()[0]->as<Operator>()->getId());
    EXPECT_TRUE(actualRootOperators[0]->getChildren()[0].get() == actualRootOperators[1]->getChildren()[0].get());
    std::vector<NodePtr> sourceOperatorsForRoot1 = actualRootOperators[0]->getAllLeafNodes();
    std::vector<NodePtr> sourceOperatorsForRoot2 = actualRootOperators[1]->getAllLeafNodes();
    EXPECT_TRUE(sourceOperatorsForRoot1.size() == 2);
    EXPECT_TRUE(sourceOperatorsForRoot2.size() == 2);
    for (const auto& sourceOperatorForRoot1 : sourceOperatorsForRoot1) {
        bool found = false;
        for (const auto& sourceOperatorForRoot2 : sourceOperatorsForRoot2) {
            if (sourceOperatorForRoot1->equal(sourceOperatorForRoot2)) {
                found = true;
            }
        }
        EXPECT_TRUE(found);
    }
}

TEST_F(SerializationUtilTest, testSerializeDeserializeCilentOriginatedQueryPlan) {

    auto query = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();

    auto serializedQueryPlan = new SerializableQueryPlan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan, true);

    auto deserializedQueryPlan = QueryPlanSerializationUtil::deserializeQueryPlan(serializedQueryPlan);

    // Expect that the root operator from the original and deserialized query plan are the same
    EXPECT_TRUE(deserializedQueryPlan->getRootOperators()[0]->equal(queryPlan->getRootOperators()[0]));

    // Expect that the child of the root operator from the original and deserialized query plan are the same
    // i.e., the source operator from  original and deserialized query plan are the same
    EXPECT_TRUE(deserializedQueryPlan->getRootOperators()[0]->getChildren()[0]->equal(
        queryPlan->getRootOperators()[0]->getChildren()[0]));

    // Expect that the id of operators in the deserialized query plan are different to the original query plan, because the initial IDs are client-generated and NES should provide its own IDs
    EXPECT_FALSE(queryPlan->getRootOperators()[0]->getId() == deserializedQueryPlan->getRootOperators()[0]->getId());
    EXPECT_FALSE(queryPlan->getRootOperators()[0]->getChildren()[0]->as<LogicalOperator>()->getId()
                 == deserializedQueryPlan->getRootOperators()[0]->getChildren()[0]->as<LogicalOperator>()->getId());
}
