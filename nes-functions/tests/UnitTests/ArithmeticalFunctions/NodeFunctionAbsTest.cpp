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
#include <Functions/ArithmeticalFunctions/NodeFunctionAbs.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class NodeFunctionAbsTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("NodeFunctionAbsTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup NodeFunctionAbsTest test class.");
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

TEST_F(NodeFunctionAbsTest, validateBeforeLoweringDifferentChildNumbers)
{
    {
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionAbs = NodeFunctionAbs::create(nodeFunctionRead);
        nodeFunctionAbs->inferStamp(dummySchema);
        nodeFunctionAbs->removeChildren();
        EXPECT_FALSE(nodeFunctionAbs->validateBeforeLowering());
    }

    {
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionAbs = NodeFunctionAbs::create(nodeFunctionRead);
        nodeFunctionAbs->inferStamp(dummySchema);
        EXPECT_TRUE(nodeFunctionAbs->validateBeforeLowering());
    }

    {
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionRead2 = NodeFunctionFieldAccess::create("f32");
        const auto nodeFunctionAbs = NodeFunctionAbs::create(nodeFunctionRead);

        ///NodeFunction Adding a second child to the Abs ---> should fail
        nodeFunctionAbs->addChild(nodeFunctionRead2);
        nodeFunctionAbs->inferStamp(dummySchema);
        EXPECT_FALSE(nodeFunctionAbs->validateBeforeLowering());
    }
}

TEST_F(NodeFunctionAbsTest, validateBeforeLoweringDifferentDataTypes)
{
    {
        ///NodeFunction Abs with a child of type i64 --> should pass
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionAbs = NodeFunctionAbs::create(nodeFunctionRead);
        nodeFunctionAbs->inferStamp(dummySchema);
        EXPECT_TRUE(nodeFunctionAbs->validateBeforeLowering());
    }

    {
        ///NodeFunction Abs with a child of type f32 --> should pass
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("f32");
        const auto nodeFunctionAbs = NodeFunctionAbs::create(nodeFunctionRead);
        nodeFunctionAbs->inferStamp(dummySchema);
        EXPECT_TRUE(nodeFunctionAbs->validateBeforeLowering());
    }

    {
        ///NodeFunction Abs with a child of type bool --> should fail
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("bool");
        const auto nodeFunctionAbs = NodeFunctionAbs::create(nodeFunctionRead);
        EXPECT_ANY_THROW(nodeFunctionAbs->inferStamp(dummySchema));
        EXPECT_FALSE(nodeFunctionAbs->validateBeforeLowering());
    }

    {
        ///NodeFunction Abs with a child of type text --> should fail
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("text");
        const auto nodeFunctionAbs = NodeFunctionAbs::create(nodeFunctionRead);
        EXPECT_ANY_THROW(nodeFunctionAbs->inferStamp(dummySchema));
        EXPECT_FALSE(nodeFunctionAbs->validateBeforeLowering());
    }
}

}