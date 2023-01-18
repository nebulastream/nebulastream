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

#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>

#include <GRPC/Serialization/EndDeviceProtocolSerializationUtil.hpp>

namespace NES {
using namespace EndDeviceProtocol;
class EndDeviceProtocolSerializationUtilTests : public Testing::NESBaseTest {
    friend class EndDeviceProtocolSerializationUtil;

  protected:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("EndDeviceProtocolSerializationUtilTests.log", NES::LogLevel::LOG_DEBUG);
    }
};

TEST_F(EndDeviceProtocolSerializationUtilTests, constantValue) {

    auto int8 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::string(1, 0)));

    std::string ser_int8 = EndDeviceProtocolSerializationUtil::serializeConstantValue(int8);

    EXPECT_TRUE(ser_int8 == "\0\0\0"s);
};

TEST_F(EndDeviceProtocolSerializationUtilTests, arithmetic) {
    auto left = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::string(1, 2)));
    auto right = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::string(1, 10)));
    auto add = AddExpressionNode::create(left, right);

    auto registers = std::make_shared<std::vector<std::string>>();

    auto res = EndDeviceProtocolSerializationUtil::serializeArithmeticalExpression(add, registers);

    std::string expected;
    char expected_chars[] = {ExpressionInstructions::CONST,
                             DataTypes::INT8,
                             2,
                             ExpressionInstructions::CONST,
                             DataTypes::INT8,
                             10,
                             ExpressionInstructions::ADD};
    for (char c : expected_chars) {
        expected.push_back(c);
    }

    EXPECT_EQ(res, expected);
}

TEST_F(EndDeviceProtocolSerializationUtilTests, MapOperation) {
    auto left = FieldAccessExpressionNode::create("var");
    auto right = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::string(1, 10)));
    auto add = AddExpressionNode::create(left, right);

    auto mapexpr =
        FieldAssignmentExpressionNode::create(FieldAccessExpressionNode::create("x")->as<FieldAccessExpressionNode>(), add);

    auto map = LogicalOperatorFactory::createMapOperator(mapexpr);

    auto registers = std::make_shared<std::vector<std::string>>();
    registers->push_back("var");

    auto ser_map = EndDeviceProtocolSerializationUtil::serializeMapOperator(map, registers);

    auto expected_instructions = std::string();
    char expected_chars[] =
        {ExpressionInstructions::VAR, 0, ExpressionInstructions::CONST, DataTypes::INT8, 10, ExpressionInstructions::ADD};
    for (char c : expected_chars) {
        expected_instructions.push_back(c);
    }
    auto expected_map = MapOperation();
    expected_map.set_attribute(1);
    expected_map.mutable_function()->set_instructions(expected_instructions);

    EXPECT_EQ(ser_map.attribute(), expected_map.attribute());
    EXPECT_EQ(ser_map.function().instructions(), expected_map.function().instructions());
}

//std::string serializeNumeric(std::variant<int8_t,uint8_t,int16_t,uint16_t,int32_t,uint32_t,int64_t,uint64_t,float_t,double_t> in){
//    size_t size;
//    const char * source;
//    std::string res;
//    switch (in.index()) {
//        case 0: break;
//        case 1:{
//            size = sizeof(int8_t);
//            source = reinterpret_cast<const char*>(std::get<int8_t>(in));
//            break;
//        }
//        case 2:{
//            size = sizeof(uint8_t);
//            source = reinterpret_cast<const char*>(std::get<uint8_t>(in));
//            break;
//        }
//        case 3: {
//            size = sizeof(int16_t);
//            source = reinterpret_cast<const char*>(std::get<int16_t>(in));
//            break;
//        }
//        case 4: {
//            size = sizeof(uint16_t);
//            source = reinterpret_cast<const char*>(std::get<uint16_t>(in));
//            break;
//        }
//        case 5: {
//            size = sizeof(int32_t);
//            source = reinterpret_cast<const char*>(std::get<int32_t>(in));
//            break;
//        }
//        case 6: {
//            size = sizeof(uint32_t);
//            source = reinterpret_cast<const char*>(std::get<uint32_t>(in));
//            break;
//        }
//        case 7:{
//            size = sizeof(int64_t);
//            source = reinterpret_cast<const char*>(std::get<int64_t>(in));
//            break;
//        }
//        case 8:{
//            size = sizeof(uint64_t);
//            source = reinterpret_cast<const char*>(std::get<uint64_t>(in));
//            break;
//        }
//        case 9:{
//            size = sizeof(float_t);
//            source = reinterpret_cast<const char*>(std::get<float_t>(in));
//            break;
//        }
//        case 10: {
//            size = sizeof(int64_t);
//            source = reinterpret_cast<const char*>(std::get<int64_t>(in));
//            break;
//        }
//    }
//    res.assign(&source[0], &source[0]+size);
//
//    return res;
//
//}
}// namespace NES
