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
#include <Functions/ArithmeticalFunctions/NodeFunctionRound.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

class NodeFunctionRoundTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("NodeFunctionRoundTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup NodeFunctionRoundTest test class.");
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

TEST_F(NodeFunctionRoundTest, validateBeforeLoweringDifferentChildNumbers)
{
    {
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionRound = NodeFunctionRound::create(nodeFunctionRead);
        nodeFunctionRound->inferStamp(dummySchema);
        nodeFunctionRound->removeChildren();
        EXPECT_FALSE(nodeFunctionRound->validateBeforeLowering());
    }

    {
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionRound = NodeFunctionRound::create(nodeFunctionRead);
        nodeFunctionRound->inferStamp(dummySchema);
        EXPECT_TRUE(nodeFunctionRound->validateBeforeLowering());
    }

    {
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionRead2 = NodeFunctionFieldAccess::create("f32");
        const auto nodeFunctionRound = NodeFunctionRound::create(nodeFunctionRead);

        ///NodeFunction Adding a second child to the Round ---> should fail
        nodeFunctionRound->addChild(nodeFunctionRead2);
        nodeFunctionRound->inferStamp(dummySchema);
        EXPECT_FALSE(nodeFunctionRound->validateBeforeLowering());
    }
}

TEST_F(NodeFunctionRoundTest, validateBeforeLoweringDifferentDataTypes)
{
    {
        ///NodeFunction Round with a child of type i64 --> should pass
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionRound = NodeFunctionRound::create(nodeFunctionRead);
        nodeFunctionRound->inferStamp(dummySchema);
        EXPECT_TRUE(nodeFunctionRound->validateBeforeLowering());
    }

    {
        ///NodeFunction Round with a child of type f32 --> should pass
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("f32");
        const auto nodeFunctionRound = NodeFunctionRound::create(nodeFunctionRead);
        nodeFunctionRound->inferStamp(dummySchema);
        EXPECT_TRUE(nodeFunctionRound->validateBeforeLowering());
    }

    {
        ///NodeFunction Round with a child of type bool --> should fail
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("bool");
        const auto nodeFunctionRound = NodeFunctionRound::create(nodeFunctionRead);
        EXPECT_ANY_THROW(nodeFunctionRound->inferStamp(dummySchema));
        EXPECT_FALSE(nodeFunctionRound->validateBeforeLowering());
    }

    {
        ///NodeFunction Round with a child of type text --> should fail
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("text");
        const auto nodeFunctionRound = NodeFunctionRound::create(nodeFunctionRead);
        EXPECT_ANY_THROW(nodeFunctionRound->inferStamp(dummySchema));
        EXPECT_FALSE(nodeFunctionRound->validateBeforeLowering());
    }
}

}