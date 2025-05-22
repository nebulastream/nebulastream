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

#include <memory>
#include <API/Schema.hpp>
#include <Execution/Functions/ArithmeticalFunctions/ExecutableFunctionAdd.hpp>
#include <Execution/Functions/ArithmeticalFunctions/ExecutableFunctionDiv.hpp>
#include <Execution/Functions/ArithmeticalFunctions/ExecutableFunctionMul.hpp>
#include <Execution/Functions/ArithmeticalFunctions/ExecutableFunctionSub.hpp>
#include <Execution/Functions/LogicalFunctions/ExecutableFunctionEquals.hpp>
#include <Execution/Functions/LogicalFunctions/ExecutableFunctionNegate.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionAdd.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionCeil.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionDiv.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionMul.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionSub.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/LogicalFunctions/NodeFunctionNegate.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <QueryCompiler/Phases/Translations/FunctionProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/BasicTypes.hpp>

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
    std::shared_ptr<Schema> dummySchema;
    std::shared_ptr<NodeFunction> nodeFunctionReadLeft;
    std::shared_ptr<NodeFunction> nodeFunctionReadRight;
    std::shared_ptr<NodeFunction> nodeFunctionReadBool;
};

TEST_F(FunctionProviderTest, testLoweringCurrentlyUnsupportedFunction)
{
    const auto nodeFunctionCeil = NodeFunctionCeil::create(nodeFunctionReadLeft);
    nodeFunctionCeil->inferStamp(*dummySchema);
    EXPECT_ANY_THROW(const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionCeil));
}

TEST_F(FunctionProviderTest, testLoweringAdd)
{
    const auto nodeFunctionAdd = NodeFunctionAdd::create(nodeFunctionReadLeft, nodeFunctionReadRight);
    nodeFunctionAdd->inferStamp(*dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionAdd);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Runtime::Execution::Functions::ExecutableFunctionAdd*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringDiv)
{
    const auto nodeFunctionDiv = NodeFunctionDiv::create(nodeFunctionReadLeft, nodeFunctionReadRight);
    nodeFunctionDiv->inferStamp(*dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionDiv);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Runtime::Execution::Functions::ExecutableFunctionDiv*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringSub)
{
    const auto nodeFunctionSub = NodeFunctionSub::create(nodeFunctionReadLeft, nodeFunctionReadRight);
    nodeFunctionSub->inferStamp(*dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionSub);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Runtime::Execution::Functions::ExecutableFunctionSub*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringMul)
{
    const auto nodeFunctionMul = NodeFunctionMul::create(nodeFunctionReadLeft, nodeFunctionReadRight);
    nodeFunctionMul->inferStamp(*dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionMul);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Runtime::Execution::Functions::ExecutableFunctionMul*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringEquals)
{
    const auto nodeFunctionEquals = NodeFunctionEquals::create(nodeFunctionReadLeft, nodeFunctionReadRight);
    nodeFunctionEquals->inferStamp(*dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionEquals);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Runtime::Execution::Functions::ExecutableFunctionEquals*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringNegate)
{
    const auto nodeFunctionNegate = NodeFunctionNegate::create(nodeFunctionReadBool);
    nodeFunctionNegate->inferStamp(*dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionNegate);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Runtime::Execution::Functions::ExecutableFunctionNegate*>(executableFunction.get()));
}

}
