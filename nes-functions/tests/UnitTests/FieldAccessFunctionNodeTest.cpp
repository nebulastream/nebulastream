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
#include <Functions/FieldAccessFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

class FieldAccessFunctionNodeTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("FieldAccessFunctionNodeTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup FieldAccessFunctionNodeTest test class.");
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

TEST_F(FieldAccessFunctionNodeTest, validateBeforeLoweringDifferentChildNumbers)
{
    {
        const auto fieldAccessFunctionNode = FieldAccessFunctionNode::create("i64");
        fieldAccessFunctionNode->inferStamp(dummySchema);
        fieldAccessFunctionNode->removeChildren();
        EXPECT_FALSE(fieldAccessFunctionNode->validateBeforeLowering());
    }

    {
        const auto readFunctionNode = FieldAccessFunctionNode::create("i64");
        const auto fieldAccessFunctionNode = FieldAccessFunctionNode::create("i64");
        fieldAccessFunctionNode->inferStamp(dummySchema);
        EXPECT_TRUE(fieldAccessFunctionNode->validateBeforeLowering());
    }

    {
        const auto readFunctionNode2 = FieldAccessFunctionNode::create("i64");
        const auto fieldAccessFunctionNode = FieldAccessFunctionNode::create("i64");

        /// Adding a second child to the FieldAccessFunctionNode ---> should fail
        fieldAccessFunctionNode->addChild(readFunctionNode2);
        fieldAccessFunctionNode->inferStamp(dummySchema);
        EXPECT_FALSE(fieldAccessFunctionNode->validateBeforeLowering());
    }
}

}