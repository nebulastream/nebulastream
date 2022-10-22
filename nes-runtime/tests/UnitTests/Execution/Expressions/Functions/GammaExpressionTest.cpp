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

#include <Execution/Expressions/Functions/GammaExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class GammaExpressionTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GammaExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup GammaExpressionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TraceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TraceTest test class." << std::endl; }
};

TEST_F(SubExpressionTest, subIntegers) {
    auto expression = BinaryExpressionWrapper<GammaExpression>();

    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>((int8_t) 5));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>((int16_t) 5));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }// Int32
    {
        auto resultValue = expression.eval(Value<Int32>((int32_t) 5));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>((int64_t) 5));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(SubExpressionTest, subUnsignedIntegers) {
    auto expression = BinaryExpressionWrapper<SubExpression>();

    // UInt8
    {
        auto resultValue = expression.eval(Value<UInt8>((uint8_t) 5));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
    // UInt16
    {
        auto resultValue = expression.eval(Value<UInt16>((uint16_t) 5));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }// UInt32
    {
        auto resultValue = expression.eval(Value<UInt32>(5u));
        ASSERT_EQ(resultValue, (double) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }// UInt64
    {
        auto resultValue = expression.eval(Value<UInt64>((uint64_t) 5));
        ASSERT_EQ(resultValue, (uint64_t) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }

    {
        auto resultValue = expression.eval(Value<UInt64>((uint64_t) 5));
        ASSERT_EQ(resultValue, (uint64_t) 24);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(SubExpressionTest, subFloat) {
    auto expression = BinaryExpressionWrapper<SubExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 5.5));
        ASSERT_EQ(resultValue, (double) 52.34277778455352);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Float>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 5.5));
        ASSERT_EQ(resultValue, (double) 52.34277778455352);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

}// namespace NES::Runtime::Execution::Expressions