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
#include <Execution/Functions/LogicalFunctions/NotEqualsFunction.hpp>
#include <TestUtils/FunctionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>

namespace NES::Runtime::Execution::Functions

{

class NotEqualsFunctionTest : public Testing::BaseUnitTest
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
        NES::Logger::setupLogging("NotEqualsFunctionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NotEqualsFunctionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down NotEqualsFunctionTest test class."); }
    const BinaryFunctionWrapper<NotEqualsFunction> function;

    template <typename LHS, typename RHS>
    void testNotEqualFunction(const LHS& leftValue, const RHS& rightValue)
    {
        const auto left = VarVal(nautilus::val<LHS>(leftValue));
        const auto right = VarVal(nautilus::val<RHS>(rightValue));
        auto resultValue = function.eval(left, right);
        EXPECT_EQ(resultValue.cast<nautilus::val<bool>>(), leftValue != rightValue);

        resultValue = function.eval(right, left);
        EXPECT_EQ(resultValue.cast<nautilus::val<bool>>(), rightValue != leftValue);
    }
};

TEST_F(NotEqualsFunctionTest, signedIntegers)
{
    testNotEqualFunction<int8_t, int8_t>(rand(), rand());
    testNotEqualFunction<int8_t, int8_t>(someMagicNumber, someMagicNumber);
    testNotEqualFunction<int8_t, int8_t>(someMagicNumber, someMagicNumber + 1);
    testNotEqualFunction<int8_t, int16_t>(someMagicNumber, minI8Minus1);
    testNotEqualFunction<int8_t, int16_t>(someMagicNumber, minI8Minus1 + 1);
    testNotEqualFunction<int8_t, int32_t>(someMagicNumber, minI16Minus1);
    testNotEqualFunction<int8_t, int32_t>(someMagicNumber, minI16Minus1 + 1);
    testNotEqualFunction<int8_t, int64_t>(someMagicNumber, minI32Minus1);
    testNotEqualFunction<int8_t, int64_t>(someMagicNumber, minI32Minus1 + 1);

    testNotEqualFunction<int16_t, int16_t>(rand(), rand());
    testNotEqualFunction<int16_t, int16_t>(minI8Minus1, minI8Minus1);
    testNotEqualFunction<int16_t, int16_t>(minI8Minus1, minI8Minus1 + 1);
    testNotEqualFunction<int16_t, int32_t>(minI8Minus1, minI16Minus1);
    testNotEqualFunction<int16_t, int32_t>(minI8Minus1, minI16Minus1 + 1);
    testNotEqualFunction<int16_t, int64_t>(minI8Minus1, minI32Minus1);
    testNotEqualFunction<int16_t, int64_t>(minI8Minus1, minI32Minus1 + 1);

    testNotEqualFunction<int32_t, int32_t>(rand(), rand());
    testNotEqualFunction<int32_t, int32_t>(minI16Minus1, minI16Minus1);
    testNotEqualFunction<int32_t, int32_t>(minI16Minus1, minI16Minus1 + 1);
    testNotEqualFunction<int32_t, int64_t>(minI16Minus1, minI16Minus1);

    testNotEqualFunction<int64_t, int64_t>(rand(), rand());
    testNotEqualFunction<int64_t, int64_t>(minI32Minus1, minI32Minus1);
    testNotEqualFunction<int64_t, int64_t>(minI32Minus1, minI32Minus1 + 1);
}

TEST_F(NotEqualsFunctionTest, UnsignedIntegers)
{
    testNotEqualFunction<uint8_t, uint8_t>(rand(), rand());
    testNotEqualFunction<uint8_t, uint8_t>(someMagicNumber, someMagicNumber);
    testNotEqualFunction<uint8_t, uint8_t>(someMagicNumber, someMagicNumber + 1);
    testNotEqualFunction<uint8_t, uint16_t>(someMagicNumber, maxUI8Plus1);
    testNotEqualFunction<uint8_t, uint16_t>(someMagicNumber, maxUI8Plus1 + 1);
    testNotEqualFunction<uint8_t, uint32_t>(someMagicNumber, maxUI16Plus1);
    testNotEqualFunction<uint8_t, uint32_t>(someMagicNumber, maxUI16Plus1 + 1);
    testNotEqualFunction<uint8_t, uint64_t>(someMagicNumber, maxUI32Plus1);
    testNotEqualFunction<uint8_t, uint64_t>(someMagicNumber, maxUI32Plus1 + 1);

    testNotEqualFunction<uint16_t, uint16_t>(rand(), rand());
    testNotEqualFunction<uint16_t, uint16_t>(maxUI8Plus1, maxUI8Plus1);
    testNotEqualFunction<uint16_t, uint16_t>(maxUI8Plus1, maxUI8Plus1 + 1);
    testNotEqualFunction<uint16_t, uint32_t>(maxUI8Plus1, maxUI16Plus1);
    testNotEqualFunction<uint16_t, uint32_t>(maxUI8Plus1, maxUI16Plus1 + 1);
    testNotEqualFunction<uint16_t, uint64_t>(maxUI8Plus1, maxUI32Plus1);
    testNotEqualFunction<uint16_t, uint64_t>(maxUI8Plus1, maxUI32Plus1 + 1);

    testNotEqualFunction<uint32_t, uint32_t>(rand(), rand());
    testNotEqualFunction<uint32_t, uint32_t>(maxUI16Plus1, maxUI16Plus1);
    testNotEqualFunction<uint32_t, uint32_t>(maxUI16Plus1, maxUI16Plus1 + 1);
    testNotEqualFunction<uint32_t, uint64_t>(maxUI16Plus1, maxUI16Plus1);

    testNotEqualFunction<uint64_t, uint64_t>(rand(), rand());
    testNotEqualFunction<uint64_t, uint64_t>(maxUI32Plus1, maxUI32Plus1);
    testNotEqualFunction<uint64_t, uint64_t>(maxUI32Plus1, maxUI32Plus1 + 1);
}

TEST_F(NotEqualsFunctionTest, FloatingPoints)
{
    testNotEqualFunction<float, float>(rand(), rand());
    testNotEqualFunction<float, float>(someMagicNumber, someMagicNumber);
    testNotEqualFunction<float, float>(someMagicNumber, someMagicNumber + 0.1);
    testNotEqualFunction<float, double>(someMagicNumber, someMagicNumber);
    testNotEqualFunction<float, double>(someMagicNumber, someMagicNumber + 0.1);

    testNotEqualFunction<double, double>(rand(), rand());
    testNotEqualFunction<double, double>(someMagicNumber, someMagicNumber);
    testNotEqualFunction<double, double>(someMagicNumber, someMagicNumber + 0.1);
}

TEST_F(NotEqualsFunctionTest, VariableSizedDataTest)
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
    /// operator!=() correctly.
    const auto result = function.eval(varSizedData1K, varSizedData1K);
    EXPECT_EQ(result.cast<nautilus::val<bool>>(), false);

    const auto resultEmpty = function.eval(varSizedData1K, varSizedDataEmpty);
    EXPECT_EQ(resultEmpty.cast<nautilus::val<bool>>(), true);
}

} /// namespace NES::Runtime::Execution::Functions
