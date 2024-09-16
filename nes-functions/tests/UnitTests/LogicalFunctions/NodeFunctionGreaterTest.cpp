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
#include <Functions/LogicalFunctions/NodeFunctionGreater.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

class NodeFunctionGreaterTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("NodeFunctionGreaterTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup NodeFunctionGreaterTest test class.");
    }
    void SetUp() override
    {
        BaseUnitTest::SetUp();

        dummySchema = Schema::create()
                          ->addField("i64", BasicType::INT64)
                          ->addField("f32", BasicType::FLOAT32)
                          ->addField("bool", BasicType::BOOLEAN)
                          ->addField("text", DataTypeFactory::createText())
                          ->updateSourceName("src");
    }

protected:
    SchemaPtr dummySchema;
};

TEST_F(NodeFunctionGreaterTest, validateBeforeLoweringDifferentChildNumbers)
{
    {
        const auto nodeFunctionReadLeft = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionReadRight = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionGreater = NodeFunctionGreater::create(nodeFunctionReadLeft, nodeFunctionReadRight);
        nodeFunctionGreater->inferStamp(dummySchema);
        nodeFunctionGreater->removeChildren();
        EXPECT_FALSE(nodeFunctionGreater->validateBeforeLowering());
    }

    {
        const auto nodeFunctionReadLeft = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionReadRight = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionGreater = NodeFunctionGreater::create(nodeFunctionReadLeft, nodeFunctionReadRight);
        nodeFunctionGreater->inferStamp(dummySchema);
        nodeFunctionGreater->removeChild(nodeFunctionReadLeft);
        EXPECT_FALSE(nodeFunctionGreater->validateBeforeLowering());
    }

    {
        const auto nodeFunctionReadLeft = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionReadRight = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionGreater = NodeFunctionGreater::create(nodeFunctionReadLeft, nodeFunctionReadRight);
        nodeFunctionGreater->inferStamp(dummySchema);
        nodeFunctionGreater->removeChild(nodeFunctionReadRight);
        EXPECT_FALSE(nodeFunctionGreater->validateBeforeLowering());
    }

    {
        const auto nodeFunctionReadLeft = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionReadRight = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionGreater = NodeFunctionGreater::create(nodeFunctionReadLeft, nodeFunctionReadRight);
        nodeFunctionGreater->inferStamp(dummySchema);
        EXPECT_TRUE(nodeFunctionGreater->validateBeforeLowering());
    }
}

TEST_F(NodeFunctionGreaterTest, validateBeforeLoweringDifferentDataTypes)
{
    auto testValidateBeforeLoweringDifferentDataTypes
        = [&](const std::string& leftType, const std::string& rightType, const bool expectedValue)
    {
        const auto nodeFunctionReadLeft = NodeFunctionFieldAccess::create(leftType);
        const auto nodeFunctionReadRight = NodeFunctionFieldAccess::create(rightType);
        const auto nodeFunctionGreater = NodeFunctionGreater::create(nodeFunctionReadLeft, nodeFunctionReadRight);
        nodeFunctionGreater->inferStamp(dummySchema);
        EXPECT_EQ(nodeFunctionGreater->validateBeforeLowering(), expectedValue);
    };

    testValidateBeforeLoweringDifferentDataTypes("i64", "i64", true);
    testValidateBeforeLoweringDifferentDataTypes("i64", "f32", true);
    testValidateBeforeLoweringDifferentDataTypes("i64", "bool", true);
    testValidateBeforeLoweringDifferentDataTypes("i64", "text", false);

    testValidateBeforeLoweringDifferentDataTypes("f32", "i64", true);
    testValidateBeforeLoweringDifferentDataTypes("f32", "f32", true);
    testValidateBeforeLoweringDifferentDataTypes("f32", "bool", true);
    testValidateBeforeLoweringDifferentDataTypes("f32", "text", false);

    testValidateBeforeLoweringDifferentDataTypes("text", "i64", false);
    testValidateBeforeLoweringDifferentDataTypes("text", "f32", false);
    testValidateBeforeLoweringDifferentDataTypes("text", "bool", false);
    testValidateBeforeLoweringDifferentDataTypes("text", "text", false);

    testValidateBeforeLoweringDifferentDataTypes("bool", "i64", true);
    testValidateBeforeLoweringDifferentDataTypes("bool", "f32", true);
    testValidateBeforeLoweringDifferentDataTypes("bool", "bool", true);
    testValidateBeforeLoweringDifferentDataTypes("bool", "text", false);
}

}
