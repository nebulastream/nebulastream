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
#include <Functions/ArithmeticalFunctions/AddPhysicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/CeilLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/DivPhysicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/MulPhysicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubPhysicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/LogicalFunctionAdd.hpp>
#include <Functions/ArithmeticalFunctions/MulLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunctions/EqualsLogicalFunction.hpp>
#include <Functions/LogicalFunctions/EqualsPhysicalFunction.hpp>
#include <Functions/LogicalFunctions/NegatePhysicalFunction.hpp>
#include <Functions/LogicalFunctions/NegateLogicalFunction.hpp>
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
        LogicalFunctionReadLeft = std::make_shared<FieldAccessLogicalFunction>("f1");
        LogicalFunctionReadRight = std::make_shared<FieldAccessLogicalFunction>("f2");
        LogicalFunctionReadBool = std::make_shared<FieldAccessLogicalFunction>("f3");
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
    LogicalFunctionCeil->withInferredStamp(dummySchema);
    EXPECT_ANY_THROW(const auto physicalFunction = QueryCompilation::FunctionProvider::lowerFunction(LogicalFunctionCeil));
}

TEST_F(FunctionProviderTest, testLoweringAdd)
{
    const auto LogicalFunctionAdd = LogicalFunctionAdd::create(LogicalFunctionReadLeft, LogicalFunctionReadRight);
    LogicalFunctionAdd->withInferredStamp(dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(LogicalFunctionAdd);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Functions::AddPhysicalFunction*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringDiv)
{
    const auto DivLogicalFunction = std::make_shared<DivLogicalFunction>(LogicalFunctionReadLeft, LogicalFunctionReadRight);
    DivLogicalFunction->withInferredStamp(dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(DivLogicalFunction);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Functions::DivPhysicalFunction*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringSub)
{
    const auto SubLogicalFunction = std::make_shared<SubLogicalFunction>(LogicalFunctionReadLeft, LogicalFunctionReadRight);
    SubLogicalFunction->withInferredStamp(dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(SubLogicalFunction);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Functions::SubPhysicalFunction*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringMul)
{
    const auto MulLogicalFunction = std::make_shared<MulLogicalFunction>(LogicalFunctionReadLeft, LogicalFunctionReadRight);
    MulLogicalFunction->withInferredStamp(dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(MulLogicalFunction);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Functions::MulPhysicalFunction*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringEquals)
{
    const auto EqualsLogicalFunction = std::make_shared<EqualsLogicalFunction>(LogicalFunctionReadLeft, LogicalFunctionReadRight);
    EqualsLogicalFunction->withInferredStamp(dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(EqualsLogicalFunction);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Functions::EqualsPhysicalFunction*>(executableFunction.get()));
}

TEST_F(FunctionProviderTest, testLoweringNegate)
{
    const auto NegateLogicalFunction = std::make_shared<NegateLogicalFunction>(LogicalFunctionReadBool);
    NegateLogicalFunction->withInferredStamp(dummySchema);
    const auto executableFunction = QueryCompilation::FunctionProvider::lowerFunction(NegateLogicalFunction);
    ASSERT_TRUE(executableFunction);
    EXPECT_TRUE(dynamic_cast<Functions::NegatePhysicalFunction*>(executableFunction.get()));
}

}
