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
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Sort/BatchSortEncode.hpp>
#include <Execution/Operators/Relational/Sort/BatchSortOperatorHandler.hpp>
#include <Execution/Operators/Relational/Sort/BatchSortScan.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <random>
#include <tuple>

namespace NES::Runtime::Execution::Operators {

template<typename T>
class BatchSortScanOperatorTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("BatchSortScanOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup BatchSortScanOperatorTest test class.");
    }
};

constexpr uint32_t SEED = 42;

/**
 *  @brief Fills the state of the sort operator with random values
 *  @param handler
 *  @param numberOfRecord number of records to fill
 *  @param numberOfFields number of fields to fill
 *  @param sortFieldIndexes vector of index of column to sort on
 *  @param min min value of random
 *  @param max max value of random
 *  @param descending sort descending or ascending
 */
template<typename T>
void fillState(const std::shared_ptr<BatchSortOperatorHandler>& handler,
               const uint64_t numberOfRecord,
               const uint64_t numberOfFields,
               const std::vector<uint64_t>& sortFieldIndexes,
               const bool descending = false,
               const T min = std::numeric_limits<T>::min(),
               const T max = std::numeric_limits<T>::max()) {
    auto state = handler->getState();
    auto entrySize = sizeof(T) * (numberOfFields + sortFieldIndexes.size() /*sort cols*/);
    auto stateRef = Nautilus::Interface::PagedVectorRef(Value<MemRef>((int8_t*) state), entrySize);
    std::mt19937 random(SEED);
    for (size_t j = 0; j < numberOfRecord; ++j) {
        // create random values
        std::vector<T> recordValues(numberOfFields);
        for (size_t i = 0; i < numberOfFields; ++i) {
            if constexpr (std::is_integral_v<T>) {
                std::uniform_int_distribution<T> dis(min, max);
                recordValues[i] = static_cast<T>(dis(random));
            } else if constexpr (std::is_floating_point_v<T>) {
                std::uniform_real_distribution<T> dis(min, max);
                recordValues[i] = static_cast<T>(dis(random));
            } else {
                NES_NOT_IMPLEMENTED();
            }
        }
        // allocate entry and fill encoded fields and full record
        auto entry = stateRef.allocateEntry();
        for (size_t i = 0; i < sortFieldIndexes.size(); ++i) {
            Value<> encodedVal(encodeData<T>(recordValues[sortFieldIndexes[i]], descending));
            entry.store(encodedVal);
            entry = entry + (uint64_t) sizeof(T);
        }

        for (size_t i = 0; i < numberOfFields; ++i) {
            Value<> val(recordValues[i]);
            entry.store(val);
            entry = entry + (uint64_t) sizeof(T);
        }
    }
}

using TestTypes = ::testing::Types<std::pair<uint64_t, UInt64>,
                                   std::pair<int64_t, Int64>,
                                   std::pair<uint32_t, UInt32>,
                                   std::pair<int32_t, Int32>,
                                   std::pair<uint16_t, UInt16>,
                                   std::pair<int16_t, Int16>,
                                   std::pair<uint8_t, UInt8>,
                                   std::pair<int8_t, Int8>,
                                   std::pair<float, Nautilus::Float>,
                                   std::pair<double, Nautilus::Double>>;
TYPED_TEST_SUITE(BatchSortScanOperatorTest, TestTypes);

/**
 * @brief Tests if the sort operator sorts records
 */
TYPED_TEST(BatchSortScanOperatorTest, SortOperatorTest) {
    using NativeType = typename TypeParam::first_type;
    using NautilusType = typename TypeParam::second_type;
    constexpr auto NUM_RECORDS = 100;
    constexpr auto NUM_FIELDS = 1;

    std::shared_ptr<BufferManager> bm = std::make_shared<BufferManager>();
    std::shared_ptr<WorkerContext> wc = std::make_shared<WorkerContext>(0, bm, 100);

    auto handler =
        std::make_shared<BatchSortOperatorHandler>(std::vector<PhysicalTypePtr>({Util::getPhysicalTypePtr<NativeType>()}),
                                                   std::vector<Record::RecordFieldIdentifier>({"f1"}),
                                                   std::vector<Record::RecordFieldIdentifier>({"f1"}));
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    handler->setup(pipelineContext);
    fillState<NativeType>(handler, NUM_RECORDS, NUM_FIELDS, {0}, /* descending */ false);

    auto sortScanOperator = BatchSortScan(0, {Util::getPhysicalTypePtr<NativeType>()}, {"f1"}, {"f1"});
    auto collector = std::make_shared<CollectOperator>();
    sortScanOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    sortScanOperator.setup(ctx);
    auto tupleBuffer = bm->getBufferBlocking();
    auto record = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
    sortScanOperator.open(ctx, record);

    ASSERT_EQ(collector->records.size(), NUM_RECORDS);
    ASSERT_EQ(collector->records[0].numberOfFields(), 1);

    // Records should be in ascending order
    auto prev = collector->records[0].read("f1").as<NautilusType>();
    for (int i = 1; i < NUM_RECORDS; ++i) {
        Value<NautilusType> cur = collector->records[i].read("f1").as<NautilusType>();
        std::cout << "prev: " << prev << " cur: " << cur << std::endl;
        ASSERT_LE(prev, cur);
        prev = cur;
    }
}

/**
 * @brief Tests if the sort operator sorts correctly when the sort field is not the first column
 */
TYPED_TEST(BatchSortScanOperatorTest, SortOperatorOnSecondColumnTest) {
    using NativeType = typename TypeParam::first_type;
    using NautilusType = typename TypeParam::second_type;
    constexpr auto NUM_RECORDS = 100;
    constexpr auto NUM_FIELDS = 2;

    std::shared_ptr<BufferManager> bm = std::make_shared<BufferManager>();
    std::shared_ptr<WorkerContext> wc = std::make_shared<WorkerContext>(0, bm, 100);

    auto pType = Util::getPhysicalTypePtr<NativeType>();
    auto handler = std::make_shared<BatchSortOperatorHandler>(std::vector<PhysicalTypePtr>({pType, pType}),
                                                              std::vector<Record::RecordFieldIdentifier>({"f1", "f2"}),
                                                              std::vector<Record::RecordFieldIdentifier>({"f2"}));
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    handler->setup(pipelineContext);
    fillState<NativeType>(handler, NUM_RECORDS, NUM_FIELDS, {1}, /* descending */ false);

    auto sortScanOperator = BatchSortScan(0, {pType, pType}, {"f1", "f2"}, {"f2"});
    auto collector = std::make_shared<CollectOperator>();
    sortScanOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    sortScanOperator.setup(ctx);
    auto tupleBuffer = bm->getBufferBlocking();
    auto record = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
    sortScanOperator.open(ctx, record);

    ASSERT_EQ(collector->records.size(), NUM_RECORDS);
    ASSERT_EQ(collector->records[0].numberOfFields(), 2);

    // Records in f2 should be in ascending order
    auto prev = collector->records[0].read("f2").as<NautilusType>();
    for (int i = 1; i < NUM_RECORDS; ++i) {
        Value<NautilusType> cur = collector->records[i].read("f2").as<NautilusType>();
        ASSERT_LE(prev, cur);
        prev = cur;
    }
}

/**
 * @brief Tests if the sort operator sorts correctly in descending order
 */
TYPED_TEST(BatchSortScanOperatorTest, SortOperatorDescendingTest) {
    using NativeType = typename TypeParam::first_type;
    using NautilusType = typename TypeParam::second_type;
    constexpr auto NUM_RECORDS = 100;
    constexpr auto NUM_FIELDS = 1;

    std::shared_ptr<BufferManager> bm = std::make_shared<BufferManager>();
    std::shared_ptr<WorkerContext> wc = std::make_shared<WorkerContext>(0, bm, 100);

    auto handler =
        std::make_shared<BatchSortOperatorHandler>(std::vector<PhysicalTypePtr>({Util::getPhysicalTypePtr<NativeType>()}),
                                                   std::vector<Record::RecordFieldIdentifier>({"f1"}),
                                                   std::vector<Record::RecordFieldIdentifier>({"f1"}));
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    handler->setup(pipelineContext);
    fillState<NativeType>(handler, NUM_RECORDS, NUM_FIELDS, {0}, /* descending */ true);

    auto sortScanOperator = BatchSortScan(0, {Util::getPhysicalTypePtr<NativeType>()}, {"f1"}, {"f1"});
    auto collector = std::make_shared<CollectOperator>();
    sortScanOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    sortScanOperator.setup(ctx);
    auto tupleBuffer = bm->getBufferBlocking();
    auto record = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
    sortScanOperator.open(ctx, record);

    ASSERT_EQ(collector->records.size(), NUM_RECORDS);
    ASSERT_EQ(collector->records[0].numberOfFields(), 1);

    // Records should be in descending order
    auto prev = collector->records[0].read("f1").as<NautilusType>();
    for (int i = 1; i < NUM_RECORDS; ++i) {
        Value<NautilusType> cur = collector->records[i].read("f1").as<NautilusType>();
        ASSERT_GE(prev, cur);
        prev = cur;
    }
}

/**
 * @brief Tests if the sort operator sorts correctly when there exist multiple fields
 */
TYPED_TEST(BatchSortScanOperatorTest, SortOperatorOnMultipleColumnsTest) {
    using NativeType = typename TypeParam::first_type;
    using NautilusType = typename TypeParam::second_type;
    constexpr auto NUM_RECORDS = 100;
    constexpr auto NUM_FIELDS = 2;

    std::shared_ptr<BufferManager> bm = std::make_shared<BufferManager>();
    std::shared_ptr<WorkerContext> wc = std::make_shared<WorkerContext>(0, bm, 100);

    auto pType = Util::getPhysicalTypePtr<NativeType>();
    auto handler = std::make_shared<BatchSortOperatorHandler>(std::vector<PhysicalTypePtr>({pType, pType}),
                                                              std::vector<Record::RecordFieldIdentifier>({"f1", "f2"}),
                                                              std::vector<Record::RecordFieldIdentifier>({"f2", "f1"}));
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    handler->setup(pipelineContext);
    fillState<NativeType>(handler, NUM_RECORDS, NUM_FIELDS, {1, 0}, /* descending */ false, 0, 5);

    auto sortScanOperator = BatchSortScan(0, {pType, pType}, {"f1", "f2"}, {"f2", "f1"});
    auto collector = std::make_shared<CollectOperator>();
    sortScanOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    sortScanOperator.setup(ctx);
    auto tupleBuffer = bm->getBufferBlocking();
    auto record = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
    sortScanOperator.open(ctx, record);

    ASSERT_EQ(collector->records.size(), NUM_RECORDS);
    ASSERT_EQ(collector->records[0].numberOfFields(), 2);

    // Records in f2, f1 should be in ascending order
    auto prev1 = collector->records[0].read("f1").as<NautilusType>();
    auto prev2 = collector->records[0].read("f2").as<NautilusType>();
    for (int i = 1; i < NUM_RECORDS; ++i) {
        Value<NautilusType> cur1 = collector->records[i].read("f1").as<NautilusType>();
        Value<NautilusType> cur2 = collector->records[i].read("f2").as<NautilusType>();
        ASSERT_LE(prev2, cur2);
        // If the first fields are equal we compare the second field
        if (prev2 == cur2) {
            ASSERT_LE(prev1, cur1);
        }
        prev1 = cur1;
        prev2 = cur2;
    }
}

/**
 * @brief Tests if the sort operator sorts records when there are multiple pages
 */
TYPED_TEST(BatchSortScanOperatorTest, SortOperatorTestMultiPage) {
    using NativeType = typename TypeParam::first_type;
    using NautilusType = typename TypeParam::second_type;
    constexpr int NUM_RECORDS =
        Interface::PagedVector::PAGE_SIZE / sizeof(NativeType) * 2 + sizeof(NativeType);// 2 full pages + 1 record -> 3 Pages
    constexpr auto NUM_FIELDS = 1;

    std::shared_ptr<BufferManager> bm = std::make_shared<BufferManager>();
    std::shared_ptr<WorkerContext> wc = std::make_shared<WorkerContext>(0, bm, 100);

    auto handler =
        std::make_shared<BatchSortOperatorHandler>(std::vector<PhysicalTypePtr>({Util::getPhysicalTypePtr<NativeType>()}),
                                                   std::vector<Record::RecordFieldIdentifier>({"f1"}),
                                                   std::vector<Record::RecordFieldIdentifier>({"f1"}));
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    handler->setup(pipelineContext);
    fillState<NativeType>(handler, NUM_RECORDS, NUM_FIELDS, {0}, /* descending */ false);

    auto sortScanOperator = BatchSortScan(0, {Util::getPhysicalTypePtr<NativeType>()}, {"f1"}, {"f1"});
    auto collector = std::make_shared<CollectOperator>();
    sortScanOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    sortScanOperator.setup(ctx);
    auto tupleBuffer = bm->getBufferBlocking();
    auto record = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
    sortScanOperator.open(ctx, record);

    ASSERT_EQ(collector->records.size(), NUM_RECORDS);
    ASSERT_EQ(collector->records[0].numberOfFields(), 1);

    // Records should be in ascending order
    auto prev = collector->records[0].read("f1").as<NautilusType>();
    for (int i = 1; i < NUM_RECORDS; ++i) {
        Value<NautilusType> cur = collector->records[i].read("f1").as<NautilusType>();
        std::cout << "prev: " << prev << " cur: " << cur << std::endl;
        ASSERT_LE(prev, cur);
        prev = cur;
    }
}

}// namespace NES::Runtime::Execution::Operators