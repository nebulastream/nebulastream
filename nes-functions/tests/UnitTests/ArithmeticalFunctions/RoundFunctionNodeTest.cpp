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
#include <Functions/ArithmeticalFunctions/RoundFunctionNode.hpp>
#include <Functions/FieldAccessFunctionNode.hpp>
#include <Functions/FieldAssignmentFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

class RoundFunctionNodeTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("RoundFunctionNodeTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup RoundFunctionNodeTest test class.");
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

TEST_F(RoundFunctionNodeTest, validateBeforeLoweringDifferentChildNumbers)
{
    {
        const auto readFunctionNode = FieldAccessFunctionNode::create("i64");
        const auto roundFunctionNode = RoundFunctionNode::create(readFunctionNode);
        roundFunctionNode->inferStamp(dummySchema);
        roundFunctionNode->removeChildren();
        EXPECT_FALSE(roundFunctionNode->validateBeforeLowering());
    }

    {
        const auto readFunctionNode = FieldAccessFunctionNode::create("i64");
        const auto roundFunctionNode = RoundFunctionNode::create(readFunctionNode);
        roundFunctionNode->inferStamp(dummySchema);
        EXPECT_TRUE(roundFunctionNode->validateBeforeLowering());
    }

    {
        const auto readFunctionNode = FieldAccessFunctionNode::create("i64");
        const auto readFunctionNode2 = FieldAccessFunctionNode::create("f32");
        const auto roundFunctionNode = RoundFunctionNode::create(readFunctionNode);

        /// Adding a second child to the RoundFunctionNode ---> should fail
        roundFunctionNode->addChild(readFunctionNode2);
        roundFunctionNode->inferStamp(dummySchema);
        EXPECT_FALSE(roundFunctionNode->validateBeforeLowering());
    }
}

TEST_F(RoundFunctionNodeTest, validateBeforeLoweringDifferentDataTypes)
{
    {
        /// RoundFunctionNode with a child of type i64 --> should pass
        const auto readFunctionNode = FieldAccessFunctionNode::create("i64");
        const auto roundFunctionNode = RoundFunctionNode::create(readFunctionNode);
        roundFunctionNode->inferStamp(dummySchema);
        EXPECT_TRUE(roundFunctionNode->validateBeforeLowering());
    }

    {
        /// RoundFunctionNode with a child of type f32 --> should pass
        const auto readFunctionNode = FieldAccessFunctionNode::create("f32");
        const auto roundFunctionNode = RoundFunctionNode::create(readFunctionNode);
        roundFunctionNode->inferStamp(dummySchema);
        EXPECT_TRUE(roundFunctionNode->validateBeforeLowering());
    }

    {
        /// RoundFunctionNode with a child of type bool --> should fail
        const auto readFunctionNode = FieldAccessFunctionNode::create("bool");
        const auto roundFunctionNode = RoundFunctionNode::create(readFunctionNode);
        EXPECT_ANY_THROW(roundFunctionNode->inferStamp(dummySchema));
        EXPECT_FALSE(roundFunctionNode->validateBeforeLowering());
    }

    {
        /// RoundFunctionNode with a child of type text --> should fail
        const auto readFunctionNode = FieldAccessFunctionNode::create("text");
        const auto roundFunctionNode = RoundFunctionNode::create(readFunctionNode);
        EXPECT_ANY_THROW(roundFunctionNode->inferStamp(dummySchema));
        EXPECT_FALSE(roundFunctionNode->validateBeforeLowering());
    }
}

}