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

#include <API/Schema.hpp>
#include <Functions/ConstantValueFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

class ConstantValueFunctionNodeTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("ConstantValueFunctionNodeTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup ConstantValueFunctionNodeTest test class.");
    }
    void SetUp() override
    {
        BaseUnitTest::SetUp();
    }
};

TEST_F(ConstantValueFunctionNodeTest, validateBeforeLoweringDifferentChildNumbers)
{
    {
        const auto constantValueFunctionNode = ConstantValueFunctionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));;
        constantValueFunctionNode->removeChildren();
        EXPECT_TRUE(constantValueFunctionNode->validateBeforeLowering());
    }

    {
        const auto constantValueFunctionNode = ConstantValueFunctionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));;
        EXPECT_TRUE(constantValueFunctionNode->validateBeforeLowering());
    }

    {
        const auto constantValueFunctionNode = ConstantValueFunctionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));;
        const auto constantValueFunctionNode2 = ConstantValueFunctionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));;

        /// Adding a child to the ConstantValueFunctionNode ---> should fail
        constantValueFunctionNode->addChild(constantValueFunctionNode2);
        EXPECT_FALSE(constantValueFunctionNode->validateBeforeLowering());
    }
}

}