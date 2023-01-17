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
