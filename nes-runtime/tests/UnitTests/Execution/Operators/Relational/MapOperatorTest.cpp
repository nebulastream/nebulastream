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

#include <BaseIntegrationTest.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class MapOperatorTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MapOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MapOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down MapOperatorTest test class."); }
};

/**
 * @brief Tests if the map operator creates a new field.
 */
TEST_F(MapOperatorTest, createNewFieldTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto addExpression = std::make_shared<Expressions::AddExpression>(readF1, readF2);
    auto writeF3 = std::make_shared<Expressions::WriteFieldExpression>("f3", addExpression);
    auto mapOperator = Map(writeF3);
    auto collector = std::make_shared<CollectOperator>();
    mapOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    auto record = Record({{"f1", Value<>(10)}, {"f2", Value<>(12)}});
    mapOperator.execute(ctx, record);

    ASSERT_EQ(collector->records.size(), 1);
    ASSERT_EQ(collector->records[0].numberOfFields(), 3);
    ASSERT_TRUE(collector->records[0].hasField("f3"));
    ASSERT_EQ(collector->records[0].read("f3"), 22);
}

/**
 * @brief Tests if the map operator overrides a field if it already exists.
 */
TEST_F(MapOperatorTest, overrideFieldTest) {
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto addExpression = std::make_shared<Expressions::AddExpression>(readF1, readF2);
    auto writeF3 = std::make_shared<Expressions::WriteFieldExpression>("f1", addExpression);
    auto mapOperator = Map(writeF3);
    auto collector = std::make_shared<CollectOperator>();
    mapOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    auto record = Record({{"f1", Value<>(10)}, {"f2", Value<>(12)}});
    mapOperator.execute(ctx, record);

    ASSERT_EQ(collector->records.size(), 1);
    ASSERT_EQ(collector->records[0].numberOfFields(), 2);
    ASSERT_TRUE(collector->records[0].hasField("f1"));
    ASSERT_EQ(collector->records[0].read("f1"), 22);
}

}// namespace NES::Runtime::Execution::Operators