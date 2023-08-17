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

#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class SelectionOperatorTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SelectionOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SelectionOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SelectionOperatorTest test class."); }
};

/**
 * @brief Tests the selection operator with qualifying record.
 */
TEST_F(SelectionOperatorTest, qualifingRecordTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto equalsExpression = std::make_shared<Expressions::EqualsExpression>(readF1, readF2);
    auto selectionOperator = Selection(equalsExpression);
    auto collector = std::make_shared<CollectOperator>();
    selectionOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    auto record = Record({{"f1", Value<>(10)}, {"f2", Value<>(10)}});
    selectionOperator.execute(ctx, record);

    ASSERT_EQ(collector->records.size(), 1);
    ASSERT_EQ(collector->records[0].numberOfFields(), 2);
    ASSERT_EQ(collector->records[0].read("f1"), 10);
    ASSERT_EQ(collector->records[0].read("f2"), 10);
}

/**
 * @brief Tests the selection operator with non qualifying record.
 */
TEST_F(SelectionOperatorTest, nonqualifingRecordTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto equalsExpression = std::make_shared<Expressions::EqualsExpression>(readF1, readF2);
    auto selectionOperator = Selection(equalsExpression);
    auto collector = std::make_shared<CollectOperator>();
    selectionOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    auto record = Record({{"f1", Value<>(10)}, {"f2", Value<>(12)}});
    selectionOperator.execute(ctx, record);
    ASSERT_EQ(collector->records.size(), 0);
}

/**
 * @brief Tests selection on non boolean field -> expect exception
 */
TEST_F(SelectionOperatorTest, wrongSelectionTypeTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto selectionOperator = Selection(readF1);
    auto collector = std::make_shared<CollectOperator>();
    selectionOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    auto record = Record({{"f1", Value<>(10)}, {"f2", Value<>(12)}});
    ASSERT_ANY_THROW(selectionOperator.execute(ctx, record));
}

}// namespace NES::Runtime::Execution::Operators