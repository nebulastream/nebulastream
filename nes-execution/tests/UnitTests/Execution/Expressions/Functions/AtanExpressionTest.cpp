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

#include <cmath>
#include <string>
#include <Execution/Expressions/Functions/AtanExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <__math/inverse_trigonometric_functions.h>
#include <gtest/gtest.h>

#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/Boolean.hpp>
#include <Nautilus/Interface/DataTypes/Float/Double.hpp>
#include <Nautilus/Interface/DataTypes/Float/Float.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Util/CastUtils.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>

namespace NES::Runtime::Execution::Expressions
{

class AtanExpressionTest : public Testing::BaseUnitTest
{
public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("AtanExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AtanExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down AtanExpressionTest test class."); }
};

TEST_F(AtanExpressionTest, evaluateAtanExpressionDouble)
{
    auto expression = UnaryExpressionWrapper<AtanExpression>();
    /// Double
    {
        auto resultValue = expression.eval(Value<Double>(0.5));
        ASSERT_EQ(resultValue, std::atan(0.5));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

TEST_F(AtanExpressionTest, evaluateAtanExpressionFloat)
{
    auto expression = UnaryExpressionWrapper<AtanExpression>();
    /// Float
    {
        auto resultValue = expression.eval(Value<Float>((float)0.5));
        ASSERT_EQ(resultValue, std::atan((float)0.5));
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

/**
* @brief If we execute the expression on a boolean it should throw an exception.
*/
TEST_F(AtanExpressionTest, evaluateAtanExpressionOnWrongType)
{
    auto expression = UnaryExpressionWrapper<AtanExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true)););
    ASSERT_ANY_THROW(expression.eval(Value<Int8>((Int8)1)););
}

} /// namespace NES::Runtime::Execution::Expressions
