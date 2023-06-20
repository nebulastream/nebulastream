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
#include <Execution/Operators/Relational/BatchDistinct.hpp>
#include <Execution/Operators/Relational/BatchDistinctOperatorHandler.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

template<typename T>
class BatchDistinctOperatorTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("BatchDistinctOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO2("Setup BatchDistinctOperatorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        bm = std::make_shared<BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }
    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO2("Tear down BatchDistinctOperatorTest test class."); }
};

using TestTypes = ::testing::Types<uint32_t>;
// using TestTypes = ::testing::Types<uint32_t, int32_t, uint64_t, int64_t, uint16_t, int16_t, uint8_t, int8_t>;
TYPED_TEST_SUITE(BatchDistinctOperatorTest, TestTypes);

/**
 * @brief Tests if the sort operator collects records with multiple fields
 */
TYPED_TEST(BatchDistinctOperatorTest, distinctOnOneField) {
    // Todo: This does NOT check the functionality of DISTINCT, but only whether the correct state is built
    std::vector<Record> records;
    // Fill the buffer with 3 distinct values to a size of 30.
    for (size_t i = 0; i < 30; i++) {
        TypeParam val1 = i % 3;
        records.push_back(Record({{"f1", Value<>(val1)}}));
    }

    // This should be fine for now.
    auto entrySize = sizeof(TypeParam);
    auto handler = std::make_shared<BatchDistinctOperatorHandler>(entrySize);
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto type = Util::getPhysicalTypePtr<TypeParam>();
    auto dataType = std::vector<PhysicalTypePtr>{type};

    // This should also be fine for now.
    auto distinctOperator = BatchDistinct(0, dataType);
    auto collector = std::make_shared<CollectOperator>();
    distinctOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    distinctOperator.setup(ctx);

    for (auto record : records) {
        distinctOperator.execute(ctx, record);
    }

    // Check if the records have been collected in the operator handler state
    auto state = handler->getState();
    ASSERT_EQ(state->getNumberOfEntries(), 30);

    for (size_t i = 0; i < 30; i++) {
        auto entry = reinterpret_cast<TypeParam*>(state->getEntry(i));
        ASSERT_EQ(*entry, i % 3);
    }
}

/**
 * @brief Tests if the sort operator collects records over multiple pages
 */
TYPED_TEST(BatchDistinctOperatorTest, DistinctOperatorMuliplePagesTest) {
    auto numberRecords = 1025;// 1025 * 2 (Fields) * 2 Bytes = 4100 Bytes > 4096 Bytes (Page Size)
    std::vector<Record> records;
    for (int i = 0; i < numberRecords; i++) {
        records.push_back(Record({{"f1", Value<>(i % 3)}}));
    }

    auto entrySize = sizeof(int32_t) * 2;
    auto handler = std::make_shared<BatchDistinctOperatorHandler>(entrySize);
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
    auto dataTypes = std::vector<PhysicalTypePtr>{integerType, integerType};
    auto distinctOperator = BatchDistinct(0, dataTypes);
    auto collector = std::make_shared<CollectOperator>();
    distinctOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    distinctOperator.setup(ctx);

    for (auto record : records) {
        distinctOperator.execute(ctx, record);
    }

    auto state = handler->getState();
    ASSERT_EQ(state->getNumberOfEntries(), numberRecords);
}
}// namespace NES::Runtime::Execution::Operators