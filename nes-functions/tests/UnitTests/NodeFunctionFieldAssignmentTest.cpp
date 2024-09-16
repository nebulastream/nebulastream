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
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

class NodeFunctionFieldAssignmentTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("NodeFunctionFieldAssignmentTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup NodeFunctionFieldAssignmentTest test class.");
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

TEST_F(NodeFunctionFieldAssignmentTest, validateBeforeLoweringDifferentChildNumbers)
{
    {
        const auto fieldAccess = NodeFunctionFieldAccess::create("i64")->as<NodeFunctionFieldAccess>();
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("f32");
        const auto nodeFunctionFieldAssignment = NodeFunctionFieldAssignment::create(fieldAccess, nodeFunctionRead);
        nodeFunctionFieldAssignment->inferStamp(dummySchema);
        nodeFunctionFieldAssignment->removeChildren();
        EXPECT_TRUE(nodeFunctionFieldAssignment->validateBeforeLowering());
    }

    {
        const auto fieldAccess = NodeFunctionFieldAccess::create("i64")->as<NodeFunctionFieldAccess>();
        const auto nodeFunctionRead = NodeFunctionFieldAccess::create("f32");
        const auto nodeFunctionFieldAssignment = NodeFunctionFieldAssignment::create(fieldAccess, nodeFunctionRead);
        nodeFunctionFieldAssignment->inferStamp(dummySchema);
        EXPECT_FALSE(nodeFunctionFieldAssignment->validateBeforeLowering());
    }
}

}
