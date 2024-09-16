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
#include <Functions/ArithmeticalFunctions/NodeFunctionCeil.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

class NodeFunctionCeilTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("NodeFunctionCeilTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup NodeFunctionCeilTest test class.");
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

TEST_F(NodeFunctionCeilTest, validateBeforeLoweringDifferentChildNumbers)
{
    {
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionCeil = NodeFunctionCeil::create(nodeFunctionRead);
        nodeFunctionCeil->inferStamp(dummySchema);
        nodeFunctionCeil->removeChildren();
        EXPECT_FALSE(nodeFunctionCeil->validateBeforeLowering());
    }

    {
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionCeil = NodeFunctionCeil::create(nodeFunctionRead);
        nodeFunctionCeil->inferStamp(dummySchema);
        EXPECT_TRUE(nodeFunctionCeil->validateBeforeLowering());
    }

    {
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionRead2 = NodeFunctionFieldAccess::create("f32");
        const auto nodeFunctionCeil = NodeFunctionCeil::create(nodeFunctionRead);

        ///NodeFunction Adding a second child to the Ceil ---> should fail
        nodeFunctionCeil->addChild(nodeFunctionRead2);
        nodeFunctionCeil->inferStamp(dummySchema);
        EXPECT_FALSE(nodeFunctionCeil->validateBeforeLowering());
    }
}

TEST_F(NodeFunctionCeilTest, validateBeforeLoweringDifferentDataTypes)
{
    {
        ///NodeFunction Ceil with a child of type i64 --> should pass
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionCeil = NodeFunctionCeil::create(nodeFunctionRead);
        nodeFunctionCeil->inferStamp(dummySchema);
        EXPECT_TRUE(nodeFunctionCeil->validateBeforeLowering());
    }

    {
        ///NodeFunction Ceil with a child of type f32 --> should pass
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("f32");
        const auto nodeFunctionCeil = NodeFunctionCeil::create(nodeFunctionRead);
        nodeFunctionCeil->inferStamp(dummySchema);
        EXPECT_TRUE(nodeFunctionCeil->validateBeforeLowering());
    }

    {
        ///NodeFunction Ceil with a child of type bool --> should fail
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("bool");
        const auto nodeFunctionCeil = NodeFunctionCeil::create(nodeFunctionRead);
        EXPECT_ANY_THROW(nodeFunctionCeil->inferStamp(dummySchema));
        EXPECT_FALSE(nodeFunctionCeil->validateBeforeLowering());
    }

    {
        ///NodeFunction Ceil with a child of type text --> should fail
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("text");
        const auto nodeFunctionCeil = NodeFunctionCeil::create(nodeFunctionRead);
        EXPECT_ANY_THROW(nodeFunctionCeil->inferStamp(dummySchema));
        EXPECT_FALSE(nodeFunctionCeil->validateBeforeLowering());
    }
}

}
