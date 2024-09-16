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
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

class NodeFunctionFieldAccessTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("NodeFunctionFieldAccessTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup NodeFunctionFieldAccessTest test class.");
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

TEST_F(NodeFunctionFieldAccessTest, validateBeforeLoweringDifferentChildNumbers)
{
    {
        const auto nodeFunctionFieldAccess = NodeFunctionFieldAccess::create("i64");
        nodeFunctionFieldAccess->inferStamp(dummySchema);
        nodeFunctionFieldAccess->removeChildren();
        EXPECT_FALSE(nodeFunctionFieldAccess->validateBeforeLowering());
    }

    {
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionFieldAccess = NodeFunctionFieldAccess::create("i64");
        nodeFunctionFieldAccess->inferStamp(dummySchema);
        EXPECT_TRUE(nodeFunctionFieldAccess->validateBeforeLowering());
    }

    {
        const auto nodeFunctionRead2 = NodeFunctionFieldAccess::create("i64");
        const auto nodeFunctionFieldAccess = NodeFunctionFieldAccess::create("i64");

        ///NodeFunction Adding a second child to the FieldAccess ---> should fail
        nodeFunctionFieldAccess->addChild(nodeFunctionRead2);
        nodeFunctionFieldAccess->inferStamp(dummySchema);
        EXPECT_FALSE(nodeFunctionFieldAccess->validateBeforeLowering());
    }
}

}