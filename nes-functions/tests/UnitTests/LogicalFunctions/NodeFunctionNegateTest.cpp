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
#include <Functions/LogicalFunctions/NodeFunctionNegate.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

class NodeFunctionNegateTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("NodeFunctionNegateTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup NodeFunctionNegateTest test class.");
    }
    void SetUp() override
    {
        BaseUnitTest::SetUp();

        dummySchema = Schema::create()
                          ->addField("i64", BasicType::INT64)
                          ->addField("f32", BasicType::FLOAT32)
                          ->addField("bool", BasicType::BOOLEAN)
                          ->addField("bool2", BasicType::BOOLEAN)
                          ->addField("text", DataTypeFactory::createText())
                          ->updateSourceName("src");
    }

protected:
    SchemaPtr dummySchema;
};

TEST_F(NodeFunctionNegateTest, validateBeforeLoweringDifferentChildNumbers)
{
    {
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("bool");
        const auto nodeFunctionNegate = NodeFunctionNegate::create(nodeFunctionRead);
        nodeFunctionNegate->inferStamp(dummySchema);
        nodeFunctionNegate->removeChildren();
        EXPECT_FALSE(nodeFunctionNegate->validateBeforeLowering());
    }

    {
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("bool");
        const auto nodeFunctionNegate = NodeFunctionNegate::create(nodeFunctionRead);
        nodeFunctionNegate->inferStamp(dummySchema);
        EXPECT_TRUE(nodeFunctionNegate->validateBeforeLowering());
    }

    {
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("bool");
        const auto nodeFunctionRead2 = NodeFunctionFieldAccess::create("bool2");
        const auto nodeFunctionNegate = NodeFunctionNegate::create(nodeFunctionRead);

        ///NodeFunction Adding a second child to the Negate ---> should fail
        nodeFunctionNegate->addChild(nodeFunctionRead2);
        nodeFunctionNegate->inferStamp(dummySchema);
        EXPECT_FALSE(nodeFunctionNegate->validateBeforeLowering());
    }
}

TEST_F(NodeFunctionNegateTest, validateBeforeLoweringDifferentDataTypes)
{
    {
        ///NodeFunction Negate with a child of type i64 --> should fail
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionNegate = NodeFunctionNegate::create(nodeFunctionRead);
        EXPECT_ANY_THROW(nodeFunctionNegate->inferStamp(dummySchema));
        EXPECT_FALSE(nodeFunctionNegate->validateBeforeLowering());
    }

    {
        ///NodeFunction Negate with a child of type f32 --> should fail
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("f32");
        const auto nodeFunctionNegate = NodeFunctionNegate::create(nodeFunctionRead);
        EXPECT_ANY_THROW(nodeFunctionNegate->inferStamp(dummySchema));
        EXPECT_FALSE(nodeFunctionNegate->validateBeforeLowering());
    }

    {
        ///NodeFunction Negate with a child of type bool --> should pass
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("bool");
        const auto nodeFunctionNegate = NodeFunctionNegate::create(nodeFunctionRead);
        nodeFunctionNegate->inferStamp(dummySchema);
        EXPECT_TRUE(nodeFunctionNegate->validateBeforeLowering());
    }

    {
        ///NodeFunction Negate with a child of type text --> should fail
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("text");
        const auto nodeFunctionNegate = NodeFunctionNegate::create(nodeFunctionRead);
        EXPECT_ANY_THROW(nodeFunctionNegate->inferStamp(dummySchema));
        EXPECT_FALSE(nodeFunctionNegate->validateBeforeLowering());
    }
}

}