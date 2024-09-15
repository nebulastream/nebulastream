/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions or
    limitations under the License.
*/

#include <API/Schema.hpp>
#include <Functions/LogicalFunctions/OrFunctionNode.hpp>
#include <Functions/FieldAccessFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

class OrFunctionNodeTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("OrFunctionNodeTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup OrFunctionNodeTest test class.");
    }
    void SetUp() override
    {
        BaseUnitTest::SetUp();

        dummySchema = Schema::create()
                          ->addField("i64", BasicType::INT64)
                          ->addField("f32", BasicType::FLOAT32)
                          ->addField("bool", BasicType::BOOLEAN)
                          ->addField("text", DataTypeFactory::createText());
    }

protected:
    SchemaPtr dummySchema;
};

TEST_F(OrFunctionNodeTest, validateBeforeLoweringDifferentChildNumbers)
{
    {
        const auto readFunctionNodeLeft = FieldAccessFunctionNode::create("bool");
        const auto readFunctionNodeRight = FieldAccessFunctionNode::create("bool");
        const auto orFunctionNode = OrFunctionNode::create(readFunctionNodeLeft, readFunctionNodeRight);
        orFunctionNode->inferStamp(dummySchema);
        orFunctionNode->removeChildren();
        EXPECT_TRUE(orFunctionNode->validateBeforeLowering());
    }

    {
        const auto readFunctionNodeLeft = FieldAccessFunctionNode::create("bool");
        const auto readFunctionNodeRight = FieldAccessFunctionNode::create("bool");
        const auto orFunctionNode = OrFunctionNode::create(readFunctionNodeLeft, readFunctionNodeRight);
        orFunctionNode->inferStamp(dummySchema);
        orFunctionNode->removeChild(readFunctionNodeLeft);
        EXPECT_FALSE(orFunctionNode->validateBeforeLowering());
    }

    {
        const auto readFunctionNodeLeft = FieldAccessFunctionNode::create("bool");
        const auto readFunctionNodeRight = FieldAccessFunctionNode::create("bool");
        const auto orFunctionNode = OrFunctionNode::create(readFunctionNodeLeft, readFunctionNodeRight);
        orFunctionNode->inferStamp(dummySchema);
        orFunctionNode->removeChild(readFunctionNodeRight);
        EXPECT_FALSE(orFunctionNode->validateBeforeLowering());
    }

    {
        const auto readFunctionNodeLeft = FieldAccessFunctionNode::create("bool");
        const auto readFunctionNodeRight = FieldAccessFunctionNode::create("bool");
        const auto orFunctionNode = OrFunctionNode::create(readFunctionNodeLeft, readFunctionNodeRight);
        orFunctionNode->inferStamp(dummySchema);
        EXPECT_TRUE(orFunctionNode->validateBeforeLowering());
    }
}

TEST_F(OrFunctionNodeTest, validateBeforeLoweringDifferentDataTypes)
{
    auto testValidateBeforeLoweringDifferentDataTypes = [&](const std::string& leftType, const std::string& rightType, const bool expectedValue) {
        const auto readFunctionNodeLeft = FieldAccessFunctionNode::create(leftType);
        const auto readFunctionNodeRight = FieldAccessFunctionNode::create(rightType);
        const auto orFunctionNode = OrFunctionNode::create(readFunctionNodeLeft, readFunctionNodeRight);
        orFunctionNode->inferStamp(dummySchema);
        EXPECT_EQ(orFunctionNode->validateBeforeLowering(), expectedValue);
    };

    testValidateBeforeLoweringDifferentDataTypes("i64", "i64", false);
    testValidateBeforeLoweringDifferentDataTypes("i64", "f32", false);
    testValidateBeforeLoweringDifferentDataTypes("i64", "bool", false);
    testValidateBeforeLoweringDifferentDataTypes("i64", "text", false);

    testValidateBeforeLoweringDifferentDataTypes("f32", "i64", false);
    testValidateBeforeLoweringDifferentDataTypes("f32", "f32", false);
    testValidateBeforeLoweringDifferentDataTypes("f32", "bool", false);
    testValidateBeforeLoweringDifferentDataTypes("f32", "text", false);

    testValidateBeforeLoweringDifferentDataTypes("text", "i64", false);
    testValidateBeforeLoweringDifferentDataTypes("text", "f32", false);
    testValidateBeforeLoweringDifferentDataTypes("text", "bool", false);
    testValidateBeforeLoweringDifferentDataTypes("text", "text", false);

    testValidateBeforeLoweringDifferentDataTypes("bool", "i64", false);
    testValidateBeforeLoweringDifferentDataTypes("bool", "f32", false);
    testValidateBeforeLoweringDifferentDataTypes("bool", "bool", true);
    testValidateBeforeLoweringDifferentDataTypes("bool", "text", false);
}

}