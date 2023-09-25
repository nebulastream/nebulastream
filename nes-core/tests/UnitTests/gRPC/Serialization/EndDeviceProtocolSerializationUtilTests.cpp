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
#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LoRaWANProxySourceType.hpp>
#include <EndDeviceProtocol.pb.h>
#include <BaseIntegrationTest.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LoRaWANProxySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Sources/LoRaWANProxySource.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestQuery.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>

#include <GRPC/Serialization/EndDeviceProtocolSerializationUtil.hpp>

#include <sstream>

namespace NES {
using namespace EndDeviceProtocol;
class EndDeviceProtocolSerializationUtilTests : public Testing::BaseIntegrationTest {
    //friend class EndDeviceProtocolSerializationUtil;

  public:
    /***
     * Helper function to quickly create strings out of bytes. Used to create expected expressions.
     * @param list list of 8 bit ints
     * @return string corresponding to the binary encoding of the given list
     */
    static std::string intsToString(std::initializer_list<char> list) {
        auto expected_instructions = std::string();
        for (char c : list) {
            expected_instructions.push_back(c);
        }
        return expected_instructions;
    }

  protected:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("EndDeviceProtocolSerializationUtilTests.log", NES::LogLevel::LOG_DEBUG);
    }
};

TEST_F(EndDeviceProtocolSerializationUtilTests, ConstantValueInt8) {
    // Test constant Value serialisation.
    constexpr int8_t number = (1 << 7) - 1;
    auto intNode =
        ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::to_string(number)));

    MapOperation actual;
    EndDeviceProtocolSerializationUtil::serializeConstantValue(intNode, actual.mutable_function());

    MapOperation expected;
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::CONST);
    expected.mutable_function()->Add()->set__int8(number);

    EXPECT_EQ(expected.DebugString(), actual.DebugString());
}

TEST_F(EndDeviceProtocolSerializationUtilTests, ConstantValueInt16) {
    // Test constant Value serialisation.
    constexpr int16_t number = (1 << 15) - 1;
    auto intNode =
        ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT16, std::to_string(number)));

    MapOperation actual;
    EndDeviceProtocolSerializationUtil::serializeConstantValue(intNode, actual.mutable_function());

    MapOperation expected;
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::CONST);
    expected.mutable_function()->Add()->set__int16(number);

    EXPECT_EQ(expected.DebugString(), actual.DebugString());
}

TEST_F(EndDeviceProtocolSerializationUtilTests, ConstantValueInt32) {
    // Test constant Value serialisation.
    constexpr int32_t number = (1u << 31) - 1;
    auto intNode =
        ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT32, std::to_string(number)));

    MapOperation actual;
    EndDeviceProtocolSerializationUtil::serializeConstantValue(intNode, actual.mutable_function());

    MapOperation expected;
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::CONST);
    expected.mutable_function()->Add()->set__int32(number);

    EXPECT_EQ(expected.DebugString(), actual.DebugString());
}

TEST_F(EndDeviceProtocolSerializationUtilTests, ConstantValueInt64) {
    // Test constant Value serialisation.
    constexpr int64_t number = (1ul << 63) - 1;
    auto intNode =
        ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, std::to_string(number)));

    MapOperation actual;
    EndDeviceProtocolSerializationUtil::serializeConstantValue(intNode, actual.mutable_function());

    MapOperation expected;
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::CONST);
    expected.mutable_function()->Add()->set__int64(number);

    EXPECT_EQ(expected.DebugString(), actual.DebugString());
}

TEST_F(EndDeviceProtocolSerializationUtilTests, ConstantValueUint8) {
    // Test constant Value serialisation.
    constexpr uint8_t number = (1 << 8) - 1;
    auto intNode =
        ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::UINT8, std::to_string(number)));

    MapOperation actual;
    EndDeviceProtocolSerializationUtil::serializeConstantValue(intNode, actual.mutable_function());

    MapOperation expected;
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::CONST);
    expected.mutable_function()->Add()->set__uint8(number);

    EXPECT_EQ(expected.DebugString(), actual.DebugString());
}

TEST_F(EndDeviceProtocolSerializationUtilTests, ConstantValueUint16) {
    // Test constant Value serialisation.
    constexpr uint16_t number = (1 << 16) - 1;
    auto intNode =
        ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::UINT16, std::to_string(number)));

    MapOperation actual;
    EndDeviceProtocolSerializationUtil::serializeConstantValue(intNode, actual.mutable_function());

    MapOperation expected;
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::CONST);
    expected.mutable_function()->Add()->set__uint16(number);

    EXPECT_EQ(expected.DebugString(), actual.DebugString());
}

TEST_F(EndDeviceProtocolSerializationUtilTests, ConstantValueUint32) {
    // Test constant Value serialisation.
    constexpr uint32_t number = (1l << 32) - 1;
    auto intNode =
        ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::UINT32, std::to_string(number)));

    MapOperation actual;
    EndDeviceProtocolSerializationUtil::serializeConstantValue(intNode, actual.mutable_function());

    MapOperation expected;
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::CONST);
    expected.mutable_function()->Add()->set__uint32(number);

    EXPECT_EQ(expected.DebugString(), actual.DebugString());
}

TEST_F(EndDeviceProtocolSerializationUtilTests, ConstantValueUint64) {
    // Test constant Value serialisation.
    constexpr uint64_t number = (1UL << 63);
    auto intNode =
        ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::UINT64, std::to_string(number)));

    MapOperation actual;
    EndDeviceProtocolSerializationUtil::serializeConstantValue(intNode, actual.mutable_function());

    MapOperation expected;
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::CONST);
    expected.mutable_function()->Add()->set__uint64(number);

    EXPECT_EQ(expected.DebugString(), actual.DebugString());
}

TEST_F(EndDeviceProtocolSerializationUtilTests, ConstantValueFloat) {
    // Test constant Value serialisation.
    constexpr float number = 1.0;
    auto floatNode =
        ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::FLOAT32, std::to_string(number)));

    MapOperation actual;
    EndDeviceProtocolSerializationUtil::serializeConstantValue(floatNode, actual.mutable_function());

    MapOperation expected;
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::CONST);
    expected.mutable_function()->Add()->set__float(number);

    EXPECT_EQ(expected.DebugString(), actual.DebugString());
}

TEST_F(EndDeviceProtocolSerializationUtilTests, ConstantValueDouble) {
    // Test constant Value serialisation.
    constexpr double number = 1.12345678912345678;
    std::stringstream ss;
    ss << std::fixed;
    ss << std::setprecision(17);
    ss << number;
    auto floatNode = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::FLOAT64, ss.str()));

    MapOperation actual;
    EndDeviceProtocolSerializationUtil::serializeConstantValue(floatNode, actual.mutable_function());

    MapOperation expected;
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::CONST);
    expected.mutable_function()->Add()->set__double(number);

    EXPECT_EQ(expected.DebugString(), actual.DebugString());
}

TEST_F(EndDeviceProtocolSerializationUtilTests, FieldAccess) {
    auto fax = FieldAccessExpressionNode::create("x");
    auto fay = FieldAccessExpressionNode::create("y");

    auto registers = std::vector<std::string>(1, "x");

    MapOperation expected;

    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::VAR);
    expected.mutable_function()->Add()->set__uint8(0);

    MapOperation actual;
    EndDeviceProtocolSerializationUtil::serializeFieldAccessExpression(fax, registers, actual.mutable_function());
    EXPECT_EQ(expected.DebugString(), actual.DebugString());

    actual.clear_function();
    EXPECT_THROW(EndDeviceProtocolSerializationUtil::serializeFieldAccessExpression(fay, registers, actual.mutable_function()),
                 EndDeviceProtocolSerializationUtil::UnsupportedEDSerialisationException);
}

TEST_F(EndDeviceProtocolSerializationUtilTests, Arithmetic) {
    //Test serialization of arithmetic operators.
    //First set up Query
    auto left = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::to_string(2)));
    auto right = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::to_string(10)));
    auto add = AddExpressionNode::create(left, right);

    //Set up registers
    auto registers = std::vector<std::string>();

    MapOperation actual;
    EndDeviceProtocolSerializationUtil::serializeArithmeticalExpression(add, registers, actual.mutable_function());

    //build expected string
    //Serialize
    MapOperation expected;
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::CONST);
    expected.mutable_function()->Add()->set__int8(2);
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::CONST);
    expected.mutable_function()->Add()->set__int8(10);
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::ADD);
    EXPECT_EQ(expected.DebugString(), actual.DebugString());
}

TEST_F(EndDeviceProtocolSerializationUtilTests, Logical) {
    auto registers = std::vector<std::string>(std::vector<std::string>{"x", "y"});

    auto left1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::to_string(12)));
    auto right1 = FieldAccessExpressionNode::create("x");
    auto eq = EqualsExpressionNode::create(left1, right1);
    auto neg = NegateExpressionNode::create(eq);

    auto left2 = FieldAccessExpressionNode::create("y");
    auto right2 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::to_string(1)));
    auto gt = GreaterExpressionNode::create(left2, right2);

    auto bor = OrExpressionNode::create(neg, gt);

    MapOperation expected;
    auto expected_function = expected.mutable_function();
    expected_function->Add()->set_instruction(ExpressionInstructions::CONST);
    expected_function->Add()->set__int8(12);
    expected_function->Add()->set_instruction(ExpressionInstructions::VAR);
    expected_function->Add()->set__uint8(0);
    expected_function->Add()->set_instruction(ExpressionInstructions::EQ);
    expected_function->Add()->set_instruction(ExpressionInstructions::NOT);
    expected_function->Add()->set_instruction(ExpressionInstructions::VAR);
    expected_function->Add()->set__uint8(1);
    expected_function->Add()->set_instruction(ExpressionInstructions::CONST);
    expected_function->Add()->set__int8(1);
    expected_function->Add()->set_instruction(ExpressionInstructions::GT);
    expected_function->Add()->set_instruction(ExpressionInstructions::OR);

    MapOperation actual;
    EndDeviceProtocolSerializationUtil::serializeLogicalExpression(bor, registers, actual.mutable_function());
    EXPECT_EQ(expected.DebugString(), actual.DebugString());
}

TEST_F(EndDeviceProtocolSerializationUtilTests, MapOperation) {
    //set up query. Note that we do a field access read
    auto left = FieldAccessExpressionNode::create("var");
    auto right = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::to_string(10)));
    auto add = AddExpressionNode::create(left, right);

    //set up mapexpression. Note that we save result to a new variable, which should be reflected as a new field in the register-map
    auto mapexpr =
        FieldAssignmentExpressionNode::create(FieldAccessExpressionNode::create("x")->as<FieldAccessExpressionNode>(), add);

    auto map = LogicalOperatorFactory::createMapOperator(mapexpr);

    //set up registers with pre-defined "var" there for the field access
    auto registers = std::vector<std::string>();
    registers.emplace_back("var");

    //serialize
    MapOperation actual;
    EndDeviceProtocolSerializationUtil::serializeMapOperator(map, registers, &actual);

    //build expected map operation
    MapOperation expected;

    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::VAR);
    expected.mutable_function()->Add()->set__uint8(0);
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::CONST);
    expected.mutable_function()->Add()->set__int8(10);
    expected.mutable_function()->Add()->set_instruction(ExpressionInstructions::ADD);
    expected.set_attribute(1);

    //compare
    EXPECT_EQ(expected.DebugString(), actual.DebugString());
    EXPECT_EQ(registers[1], "x");
}

TEST_F(EndDeviceProtocolSerializationUtilTests, FilterOperation) {
    //build expression and filter operator
    auto left = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::to_string(8)));
    auto right = FieldAccessExpressionNode::create("var");
    auto lt = LessExpressionNode::create(left, right);
    auto filter = LogicalOperatorFactory::createFilterOperator(lt);

    auto registers = std::vector<std::string>();
    registers.emplace_back("var");

    FilterOperation actual;
    EndDeviceProtocolSerializationUtil::serializeFilterOperator(filter, registers, &actual);

    //build expected protobuf

    FilterOperation expected;
    expected.mutable_predicate()->Add()->set_instruction(ExpressionInstructions::CONST);
    expected.mutable_predicate()->Add()->set__int8(8);
    expected.mutable_predicate()->Add()->set_instruction(ExpressionInstructions::VAR);
    expected.mutable_predicate()->Add()->set__uint8(0);
    expected.mutable_predicate()->Add()->set_instruction(ExpressionInstructions::LT);

    EXPECT_EQ(expected.DebugString(), actual.DebugString());
}

TEST_F(EndDeviceProtocolSerializationUtilTests, QueryPlan) {

    EndDeviceProtocol::Query expected;
    auto op = expected.mutable_operations()->Add();
    op->mutable_map()->set_attribute(0);
    op->mutable_map()->mutable_function()->Add()->set_instruction(ExpressionInstructions::VAR);
    op->mutable_map()->mutable_function()->Add()->set__uint8(0);

    auto schema = Schema::create()->addField("x", BasicType::INT8);
    auto loraSourceType = LoRaWANProxySourceType::create();
    loraSourceType->setSensorFields(std::vector<std::string>{"x"});
    auto loraDesc = LoRaWANProxySourceDescriptor::create(schema, loraSourceType, "default_logical", "default_physical");
    auto logicalSource = LogicalSource::create("default_logical", schema);

    auto query = TestQuery::from(loraDesc).map(Attribute("x") = Attribute("x")).sink(NullOutputSinkDescriptor::create());
    auto actual = EndDeviceProtocolSerializationUtil::serializeQueryPlanToEndDevice(query.getQueryPlan(), loraSourceType);

    EXPECT_EQ(expected.DebugString(), actual->DebugString());

    EXPECT_EQ(query.getQueryPlan()->getOperatorByType<MapLogicalOperatorNode>().size(), 1);
    EXPECT_EQ(query.getQueryPlan()->getSourceOperators().size(), 1);
    EXPECT_EQ(query.getQueryPlan()->getSinkOperators().size(), 1);
    EXPECT_TRUE(query.getQueryPlan()->getSourceOperators().at(0)->getSourceDescriptor()->equal(loraDesc));
    EXPECT_TRUE(query.getQueryPlan()->getSinkOperators().at(0)->getSinkDescriptor()->instanceOf<NullOutputSinkDescriptor>());
}

}// namespace NES
