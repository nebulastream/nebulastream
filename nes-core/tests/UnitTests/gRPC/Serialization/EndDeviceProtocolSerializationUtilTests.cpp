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

#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>

#include <GRPC/Serialization/EndDeviceProtocolSerializationUtil.hpp>

namespace NES {
using namespace EndDeviceProtocol;
class EndDeviceProtocolSerializationUtilTests : public Testing::NESBaseTest {
    friend class EndDeviceProtocolSerializationUtil;

  public:
    static void SetUpTestCase() { setupLogging(); }
    void SetUp() override { Testing::NESBaseTest::SetUp(); }

  protected:
    static void setupLogging() {
        NES::Logger::setupLogging("EndDeviceProtocolSerializationUtilTests.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup EndDeviceProtocolSerializationUtilTests test class.");
    }
};

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

TEST_F(EndDeviceProtocolSerializationUtilTests, constantValue) {
//    auto pF = DefaultPhysicalTypeFactory();
//    int8_t hej = 2;
    auto int8 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::string(1, 0)));
//    auto int8 = ConstantValueExpressionNode::create(
//        DataTypeFactory::createBasicValue(
//            BasicType::INT8,
//            pF.getPhysicalType(
//                  DataTypeFactory::createInt8())->convertRawToString(&hej)
//            )
//        );
    auto uint8 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::UINT8, std::string(1, 1)));
//    auto int16 = ConstantValueExpressionNode::create(
//        DataTypeFactory::createBasicValue(BasicType::INT16, std::string((int16_t) 2)));
//    auto uint16 = ConstantValueExpressionNode::create(
//        DataTypeFactory::createBasicValue(BasicType::UINT16, std::string(reinterpret_cast<const char*>((uint16_t) 3))));
//    auto int32 =
//        ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT32, std::string(1, (int32_t) 4)));
//    auto uint32 =
//        ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::UINT32, std::string(1, (uint32_t) 5)));
//    auto int64 =
//        ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT64, std::string(1, (int64_t) 6)));
//    auto uint64 =
//        ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::UINT64, std::string(1, (uint64_t) 7)));
//    auto f = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::FLOAT32, std::string(1, 0)));

    auto ser_int8 = EndDeviceProtocolSerializationUtil::serializeConstantValue(int8);
    auto ser_uint8 = EndDeviceProtocolSerializationUtil::serializeConstantValue(uint8);
//    auto ser_int16 = EndDeviceProtocolSerializationUtil::serializeConstantValue(int16);
//    auto ser_uint16 = EndDeviceProtocolSerializationUtil::serializeConstantValue(uint16);
//    auto ser_int32 = EndDeviceProtocolSerializationUtil::serializeConstantValue(int32);
//    auto ser_uint32 = EndDeviceProtocolSerializationUtil::serializeConstantValue(uint32);
//    auto ser_int64 = EndDeviceProtocolSerializationUtil::serializeConstantValue(int64);
//    auto ser_uint64 = EndDeviceProtocolSerializationUtil::serializeConstantValue(uint64);

    EXPECT_EQ(ser_int8, "\0\0\0"s);
    EXPECT_EQ(ser_uint8,"\0\1\1"s);
//    EXPECT_EQ(ser_int16, "");
//    EXPECT_EQ(ser_uint16, "");
//    EXPECT_EQ(ser_int32, "");
//    EXPECT_EQ(ser_uint32, "");
//    EXPECT_EQ(ser_int64, "");
//    EXPECT_EQ(ser_uint64, "");
}

TEST_F(EndDeviceProtocolSerializationUtilTests, arithmetic) {
    auto left = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::string(1, 2)));
    auto right = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(BasicType::INT8, std::string(1, 10)));
    auto add = AddExpressionNode::create(left, right);
    auto res = EndDeviceProtocolSerializationUtil::serializeArithmeticalExpression(add);

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

    EXPECT_EQ(EndDeviceProtocolSerializationUtil::serializeArithmeticalExpression(add), expected);
}
}// namespace NES
