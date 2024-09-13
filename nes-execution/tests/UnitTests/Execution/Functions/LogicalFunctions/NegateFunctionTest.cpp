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
#include <Execution/Functions/LogicalFunctions/NegateFunction.hpp>
#include <TestUtils/FunctionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>

namespace NES::Runtime::Execution::Functions

{

class NegateFunctionTest : public Testing::BaseUnitTest
{
public:
    /// Defining some constexpr values for more readable tests
    static constexpr auto someMagicNumber = 23.0;
    static constexpr auto minI8Minus1 = static_cast<int16_t>(std::numeric_limits<int8_t>::min()) - 1;
    static constexpr auto minI16Minus1 = static_cast<int32_t>(std::numeric_limits<int16_t>::min()) - 1;
    static constexpr auto minI32Minus1 = static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1;
    static constexpr auto maxUI8Plus1 = static_cast<uint16_t>(std::numeric_limits<uint8_t>::max()) + 1;
    static constexpr auto maxUI16Plus1 = static_cast<uint32_t>(std::numeric_limits<uint16_t>::max()) + 1;
    static constexpr auto maxUI32Plus1 = static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;


    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("NegateFunctionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NegateFunctionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down NegateFunctionTest test class."); }
    const UnaryFunctionWrapper<NegateFunction> function;

    template <typename T>
    void testNegateFunction(const T& value)
    {
        const auto varVal = VarVal(nautilus::val<T>(value));
        auto resultValue = function.eval(varVal);
        EXPECT_EQ(resultValue.cast<nautilus::val<bool>>(), !value);
    }
};

TEST_F(NegateFunctionTest, fixedDataTypes)
{
    /// Signed Integers
    testNegateFunction<int8_t>(rand());
    testNegateFunction<int16_t>(rand());
    testNegateFunction<int32_t>(rand());
    testNegateFunction<int64_t>(rand());

    /// Unsigned Integers
    testNegateFunction<uint8_t>(rand());
    testNegateFunction<uint16_t>(rand());
    testNegateFunction<uint32_t>(rand());
    testNegateFunction<uint64_t>(rand());

    /// Floating Points
    testNegateFunction<float>(rand());
    testNegateFunction<double>(rand());


    /// Booleans
    testNegateFunction<bool>(true);
    testNegateFunction<bool>(false);
}

TEST_F(NegateFunctionTest, VariableSizedDataTest)
{
    auto createVariableSizedRandomData = [](const uint32_t size)
    {
        constexpr auto sizeOfLengthInBytes = 4;
        std::vector<int8_t> content(sizeOfLengthInBytes + size);
        for (uint32_t i = 0; i < size; i++)
        {
            content[sizeOfLengthInBytes + i] = rand() % 256;
        }

        std::memcpy(content.data(), &size, sizeOfLengthInBytes);
        return content;
    };

    /// Creating a variable sized data of size 1KiB
    auto variableSizedData = createVariableSizedRandomData(1024);
    const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
    const VarVal varSizedData1K = VariableSizedData(ptrToVariableSized);

    /// Creating a variable sized data of size 0B
    auto variableSizedDataEmpty = createVariableSizedRandomData(0);
    const nautilus::val<int8_t*> ptrToVariableSizedEmpty(variableSizedDataEmpty.data());
    const VarVal varSizedDataEmpty = VariableSizedData(ptrToVariableSizedEmpty);

    /// As we test the notEquals in VariableSizedDataTest, we perform here a simple test to see if we have implemented the call to the underlying
    /// operator!() correctly.
    const auto result = function.eval(varSizedData1K);
    EXPECT_EQ(result.cast<nautilus::val<bool>>(), false);

    const auto resultEmpty = function.eval(varSizedDataEmpty);
    EXPECT_EQ(resultEmpty.cast<nautilus::val<bool>>(), true);
}

} /// namespace NES::Runtime::Execution::Functions
