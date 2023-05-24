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
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Sort/Sort.hpp>
#include <Execution/Operators/Relational/Sort/SortOperatorHandler.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
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
 * @brief Tests if the sort operator collects records with multiple fields
 */
TEST_F(SortOperatorTest, SortOperatorMultipleFieldsTest) {
    std::vector<Record> records;
    records.push_back(Record({{"f1", Value<>(50)}, {"f2", Value<>(1)}}));
    records.push_back(Record({{"f1", Value<>(40)}, {"f2", Value<>(2)}}));
    records.push_back(Record({{"f1", Value<>(30)}, {"f2", Value<>(3)}}));
    records.push_back(Record({{"f1", Value<>(20)}, {"f2", Value<>(4)}}));
    records.push_back(Record({{"f1", Value<>(10)}, {"f2", Value<>(5)}}));

    auto entrySize = sizeof(int32_t) + sizeof(int32_t);
    auto handler = std::make_shared<SortOperatorHandler>(entrySize);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
    auto dataTypes = std::vector<PhysicalTypePtr>{integerType, integerType};
    auto sortOperator = Sort(0, dataTypes);
    auto collector = std::make_shared<CollectOperator>();
    sortOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    sortOperator.setup(ctx);

    for (auto record : records) {
        sortOperator.execute(ctx, record);
    }

    // Check if the records have been collected in the operator handler state
    auto state = handler->getState();
    ASSERT_EQ(state->getNumberOfEntries(), 5);

    auto entry = reinterpret_cast<int32_t*>(state->getEntry(0));
    ASSERT_EQ(*entry, 50);
    ASSERT_EQ(*(entry + 1), 1);
    entry = reinterpret_cast<int32_t*>(state->getEntry(1));
    ASSERT_EQ(*entry, 40);
    ASSERT_EQ(*(entry + 1), 2);
    entry = reinterpret_cast<int32_t*>(state->getEntry(2));
    ASSERT_EQ(*entry, 30);
    ASSERT_EQ(*(entry + 1), 3);
    entry = reinterpret_cast<int32_t*>(state->getEntry(3));
    ASSERT_EQ(*entry, 20);
    ASSERT_EQ(*(entry + 1), 4);
    entry = reinterpret_cast<int32_t*>(state->getEntry(4));
    ASSERT_EQ(*entry, 10);
    ASSERT_EQ(*(entry + 1), 5);
}

/**
 * @brief Tests if the sort operator collects records over multiple pages
 */
TEST_F(SortOperatorTest, SortOperatorMuliplePagesTest) {
    auto numberRecords = 1025; // 1025 * 2 (Fields) * 2 Bytes = 4100 Bytes > 4096 Bytes (Page Size)
    std::vector<Record> records;
    for(int i = 0; i < numberRecords; i++) {
        records.push_back(Record({{"f1", Value<>(50)}, {"f2", Value<>(1)}}));
    }

    auto entrySize = sizeof(int32_t) + sizeof(int32_t);
    auto handler = std::make_shared<SortOperatorHandler>(entrySize);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
    auto dataTypes = std::vector<PhysicalTypePtr>{integerType, integerType};
    auto sortOperator = Sort(0, dataTypes);
    auto collector = std::make_shared<CollectOperator>();
    sortOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    sortOperator.setup(ctx);

    for (auto record : records) {
        sortOperator.execute(ctx, record);
    }

    auto state = handler->getState();
    ASSERT_EQ(state->getNumberOfEntries(), numberRecords);
}

}// namespace NES::Runtime::Execution::Operators