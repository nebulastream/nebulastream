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
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/ThresholdWindow.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class ThresholdWindowOperatorTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ThresholdWindowOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ThresholdWindowOperatorTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup ThresholdWindowOperatorTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down ThresholdWindowOperatorTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ThresholdWindowOperatorTest test class." << std::endl; }
};

/**
 * @brief Tests the threshold window operator with a sum aggregation.
 */
TEST_F(ThresholdWindowOperatorTest, hresholdWindowWithSumAggTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto fortyTwo = std::make_shared<Expressions::ConstantIntegerExpression>(42);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, fortyTwo);
    auto thresholdWindowOperator = ThresholdWindow(greaterThanExpression);
    auto collector = std::make_shared<CollectOperator>();
    thresholdWindowOperator.setChild(collector); // TODO 3138: the child should be an aggregation operator
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));

    auto recordTen = Record({{"f1", Value<>(10)}, {"f2", Value<>(1)}});
    auto recordFifty = Record({{"f1", Value<>(50)}, {"f2", Value<>(2)}});
    auto recordNinety = Record({{"f1", Value<>(90)}, {"f2", Value<>(3)}});
    thresholdWindowOperator.execute(ctx, recordTen);
    thresholdWindowOperator.execute(ctx, recordFifty);
    thresholdWindowOperator.execute(ctx, recordNinety);

    // TODO: the collector should collect from an aggregation operator, which only has one field, i.e., the aggregated value
    ASSERT_EQ(collector->records.size(), 2);
    ASSERT_EQ(collector->records[0].numberOfFields(), 2);
    ASSERT_EQ(collector->records[0].read("f1"), 50);
    ASSERT_EQ(collector->records[1].numberOfFields(), 2);
    ASSERT_EQ(collector->records[1].read("f1"), 90);

    ASSERT_EQ(thresholdWindowOperator.sum, 5);
}
}// namespace NES::Runtime::Execution::Operators