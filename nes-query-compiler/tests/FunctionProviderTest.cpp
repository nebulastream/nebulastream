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
#include <Functions/ArithmeticalFunctions/CeilUnaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/DivBinaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/ExecutableFunctionAdd.hpp>
#include <Functions/ArithmeticalFunctions/ExecutableFunctionDiv.hpp>
#include <Functions/ArithmeticalFunctions/ExecutableFunctionMul.hpp>
#include <Functions/ArithmeticalFunctions/ExecutableFunctionSub.hpp>
#include <Functions/ArithmeticalFunctions/LogicalFunctionAdd.hpp>
#include <Functions/ArithmeticalFunctions/MulBinaryLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubBinaryLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunctions/EqualsBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/ExecutableFunctionEquals.hpp>
#include <Functions/LogicalFunctions/ExecutableFunctionNegate.hpp>
#include <Functions/LogicalFunctions/NegateUnaryLogicalFunction.hpp>
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
        LogicalFunctionReadLeft = FieldAccessLogicalFunction::create("f1");
        LogicalFunctionReadRight = FieldAccessLogicalFunction::create("f2");
        LogicalFunctionReadBool = FieldAccessLogicalFunction::create("f3");
    }

protected:
    std::shared_ptr<Schema> dummySchema;
    std::shared_ptr<LogicalFunction> LogicalFunctionReadLeft;
    std::shared_ptr<LogicalFunction> LogicalFunctionReadRight;
    std::shared_ptr<LogicalFunction> LogicalFunctionReadBool;
};

TEST_F(FunctionProviderTest, testLoweringCurrentlyUnsupportedFunction)
{
    const auto LogicalFunctionCeil = LogicalFunctionCeil::create(LogicalFunctionReadLeft);
    LogicalFunctionCeil->inferStamp(dummySchema);
    EXPECT_ANY_THROW(const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(LogicalFunctionCeil));
}

TEST_F(FunctionProviderTest, testLoweringAdd)
{
    const auto LogicalFunctionAdd = LogicalFunctionAdd::create(LogicalFunctionReadLeft, LogicalFunctionReadRight);
    LogicalFunctionAdd->withInferredStamp(dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(LogicalFunctionAdd);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Functions::ExecutableFunctionAdd*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringDiv)
{
    const auto DivBinaryLogicalFunction = DivBinaryLogicalFunction::create(LogicalFunctionReadLeft, LogicalFunctionReadRight);
    DivBinaryLogicalFunction->inferStamp(dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(DivBinaryLogicalFunction);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Functions::ExecutableFunctionDiv*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringSub)
{
    const auto SubBinaryLogicalFunction = SubBinaryLogicalFunction::create(LogicalFunctionReadLeft, LogicalFunctionReadRight);
    SubBinaryLogicalFunction->inferStamp(dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(SubBinaryLogicalFunction);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Functions::ExecutableFunctionSub*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringMul)
{
    const auto MulBinaryLogicalFunction = MulBinaryLogicalFunction::create(LogicalFunctionReadLeft, LogicalFunctionReadRight);
    MulBinaryLogicalFunction->inferStamp(dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(MulBinaryLogicalFunction);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Functions::ExecutableFunctionMul*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringEquals)
{
    const auto EqualsBinaryLogicalFunction = EqualsBinaryLogicalFunction::create(LogicalFunctionReadLeft, LogicalFunctionReadRight);
    EqualsBinaryLogicalFunction->inferStamp(dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(EqualsBinaryLogicalFunction);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Functions::ExecutableFunctionEquals*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringNegate)
{
    const auto NegateUnaryLogicalFunction = NegateUnaryLogicalFunction::create(LogicalFunctionReadBool);
    NegateUnaryLogicalFunction->inferStamp(dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(NegateUnaryLogicalFunction);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Functions::ExecutableFunctionNegate*>(executableFunction.get()));
}

}
