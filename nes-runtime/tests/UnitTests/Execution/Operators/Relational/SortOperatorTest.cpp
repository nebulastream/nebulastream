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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Sort/Sort.hpp>
#include <Execution/Operators/Relational/Sort/SortOperatorHandler.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class SortOperatorTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SortOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SortOperatorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        bm = std::make_shared<BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }


    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SortOperatorTest test class."); }
};

/**
 * @brief Tests the sort operator.
 */
TEST_F(SortOperatorTest, SortOperatorTest) {
    // We always sort on the first operator
    auto handler = SortOperatorHandler(0);
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    auto stringType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createText());
    auto sortOperator = Sort(0,std::vector<PhysicalTypePtr>{integerType, stringType});
    auto collector = std::make_shared<CollectOperator>();
    sortOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    auto record = Record({{"f1", Value<>(50)}, {"f2", Value<>(1)}});
    auto record1 = Record({{"f1", Value<>(40)}, {"f2", Value<>(2)}});
    auto record2 = Record({{"f1", Value<>(30)}, {"f2", Value<>(3)}});
    auto record3 = Record({{"f1", Value<>(20)}, {"f2", Value<>(4)}});
    auto record4 = Record({{"f1", Value<>(10)}, {"f2", Value<>(5)}});
    sortOperator.execute(ctx, record);
    // TODO: Run sortScan

    // It should be sorted by the first field
    ASSERT_EQ(collector->records.size(), 5);
    ASSERT_EQ(collector->records[0].read("f1"), 10);
    ASSERT_EQ(collector->records[0].read("f2"), 5);

    ASSERT_EQ(collector->records[1].read("f1"), 20);
    ASSERT_EQ(collector->records[1].read("f2"), 4);

    ASSERT_EQ(collector->records[2].read("f1"), 30);
    ASSERT_EQ(collector->records[2].read("f2"), 3);

    ASSERT_EQ(collector->records[3].read("f1"), 40);
    ASSERT_EQ(collector->records[3].read("f2"), 2);

    ASSERT_EQ(collector->records[4].read("f1"), 50);
    ASSERT_EQ(collector->records[4].read("f2"), 1);
}

}// namespace NES::Runtime::Execution::Operators