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

#include <Execution/Expressions/Functions/FloorExpression.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

        class FloorExpressionTest : public testing::Test {
          public:
            /* Will be called before any test in this class are executed. */
            static void SetUpTestCase() {
                NES::Logger::setupLogging("FloorExpressionTest.log", NES::LogLevel::LOG_DEBUG);
                std::cout << "Setup FloorExpressionTest test class." << std::endl;
            }

            /* Will be called before a test is executed. */
            void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

            /* Will be called before a test is executed. */
            void TearDown() override { std::cout << "Tear down TraceTest test case." << std::endl; }

            /* Will be called after all tests in this class are finished. */
            static void TearDownTestCase() { std::cout << "Tear down TraceTest test class." << std::endl; }
        };

        TEST_F(FloorExpressionTest, evaluateModExpressionInteger) {
            auto expression = UnaryExpressionWrapper<FloorExpression>();
            // Int8
            {
                auto resultValue = expression.eval(Value<Int8>((int8_t) 17));
                ASSERT_EQ(resultValue, (float) 17);
                ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
            }
            // Int64
            {
                auto resultValue = expression.eval(Value<Int64>((int64_t) 17.2));
                ASSERT_EQ(resultValue, (float) 17);
                ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
            }
        }

        TEST_F(FloorExpressionTest, evaluateModExpressionFloat) {
            auto expression = UnaryExpressionWrapper<FloorExpression>();
            // Float
            {
                auto resultValue = expression.eval(Value<Float>((float) 17.9));
                ASSERT_EQ(resultValue, (float) 17);
                ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
            }
            // Float
            {
                auto resultValue = expression.eval(Value<Float>((float) 17.5));
                ASSERT_EQ(resultValue, (float) 17);
                ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
            }
        }

        /**
* @brief If we execute the expression on a boolean it should throw an exception.
*/
        TEST_F(FloorExpressionTest, evaluateModExpressionOnWrongType) {
            auto expression = UnaryExpressionWrapper<FloorExpression>();
            ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true)););
        }

    }// namespace NES::Runtime::Execution::Expressions
