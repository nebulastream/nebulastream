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
#include <Execution/Functions/LogicalFunctions/ExecutableFunctionNegate.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionCeil.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionNegate.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <QueryCompiler/Phases/Translations/FunctionProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{
class FunctionProviderTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("FunctionProviderTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup FunctionProviderTest test class.");
    }
    void SetUp() override
    {
        BaseUnitTest::SetUp();

        dummySchema = Schema::create()
                          ->addField("f1", BasicType::INT64)
                          ->addField("f2", BasicType::INT64)
                          ->addField("f3", BasicType::BOOLEAN)
                          ->updateSourceName("src");
        nodeFunctionReadLeft = NodeFunctionFieldAccess::create("f1");
        nodeFunctionReadRight = NodeFunctionFieldAccess::create("f2");
        nodeFunctionReadBool = NodeFunctionFieldAccess::create("f3");
    }

protected:
    SchemaPtr dummySchema;
    NodeFunctionPtr nodeFunctionReadLeft;
    NodeFunctionPtr nodeFunctionReadRight;
    NodeFunctionPtr nodeFunctionReadBool;
};

TEST_F(FunctionProviderTest, testLoweringCurrentlyUnsupportedFunction)
{
    const auto nodeFunctionCeil = NodeFunctionCeil::create(nodeFunctionReadLeft);
    nodeFunctionCeil->inferStamp(dummySchema);
    EXPECT_ANY_THROW(const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionCeil));
}

TEST_F(FunctionProviderTest, testLoweringNegate)
{
    const auto nodeFunctionNegate = NodeFunctionNegate::create(nodeFunctionReadBool);
    nodeFunctionNegate->inferStamp(dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionNegate);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Runtime::Execution::Functions::ExecutableFunctionNegate*>(executableFunction.get()));
}

}
