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
#include <Execution/Operators/Relational/Sort/Sort.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class SortOperatorTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SortOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SortOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SortOperatorTest test class."); }
};

/**
 * @brief Tests the sort operator.
 */
TEST_F(SortOperatorTest, SortOperatorTest) {
    // We always sort on the first operator
    auto sortOperator = Sort();
    auto collector = std::make_shared<CollectOperator>();
    sortOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    auto record = Record({{"f1", Value<>(50)}, {"f2", Value<>("A")}});
    auto record1 = Record({{"f1", Value<>(40)}, {"f2", Value<>("B")}});
    auto record2 = Record({{"f1", Value<>(30)}, {"f2", Value<>("C")}});
    auto record3 = Record({{"f1", Value<>(20)}, {"f2", Value<>("D")}});
    auto record4 = Record({{"f1", Value<>(10)}, {"f2", Value<>("E")}});
    sortOperator.execute(ctx, record);

    // It should be sorted by the first field
    ASSERT_EQ(collector->records.size(), 5);
    ASSERT_EQ(collector->records[0].read("f1"), 10);
    ASSERT_EQ(collector->records[0].read("f2"), "E");

    ASSERT_EQ(collector->records[1].read("f1"), 20);
    ASSERT_EQ(collector->records[1].read("f2"), "D");

    ASSERT_EQ(collector->records[2].read("f1"), 30);
    ASSERT_EQ(collector->records[2].read("f2"), "C");

    ASSERT_EQ(collector->records[3].read("f1"), 40);
    ASSERT_EQ(collector->records[3].read("f2"), "B");

    ASSERT_EQ(collector->records[4].read("f1"), 50);
    ASSERT_EQ(collector->records[4].read("f2"), "A");
}

}// namespace NES::Runtime::Execution::Operators