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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Sort/BatchSort.hpp>
#include <Execution/Operators/Relational/Sort/BatchSortEncode.hpp>
#include <Execution/Operators/Relational/Sort/BatchSortOperatorHandler.hpp>
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
class BatchSortOperatorTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("BatchSortOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup BatchSortOperatorTest test class.");
    }
};

using TestTypes = ::testing::Types<std::pair<uint32_t, uint32_t>,
                                   std::pair<int32_t, int32_t>,
                                   std::pair<uint64_t, uint64_t>,
                                   std::pair<int64_t, int64_t>,
                                   std::pair<uint16_t, uint16_t>,
                                   std::pair<int16_t, int16_t>,
                                   std::pair<uint8_t, uint8_t>,
                                   std::pair<int8_t, int8_t>,
                                   std::pair<float, uint32_t>,
                                   std::pair<double, uint64_t>>;
TYPED_TEST_SUITE(BatchSortOperatorTest, TestTypes);

template<typename Type, typename EncodedType>
void createRecords(std::vector<Record>& records, size_t numRecords) {
    for (size_t i = 0; i < numRecords; i++) {
        Type val1 = i * 10;
        Type val2 = i;
        records.push_back(Record({{"f1", Value<>(val1)}, {"f2", Value<>(val2)}}));
    }
}

/**
 * @brief Tests if the sort operator collects records with multiple fields
 */
TYPED_TEST(BatchSortOperatorTest, SortOperatorMultipleFieldsTest) {
    using Type = typename TypeParam::first_type;
    using EncodedType = typename TypeParam::second_type;
    constexpr size_t NUM_RECORDS = 100;

    std::vector<Record> records;
    createRecords<Type, EncodedType>(records, NUM_RECORDS);

    auto nType = Util::getPhysicalTypePtr<Type>();
    auto handler = std::make_shared<BatchSortOperatorHandler>(std::vector<PhysicalTypePtr>({nType, nType}),
                                                              std::vector<Record::RecordFieldIdentifier>({"f1", "f2"}),
                                                              std::vector<Record::RecordFieldIdentifier>({"f1"}));
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto sortOperator = BatchSort(0, {nType, nType}, {"f1", "f2"}, {"f1"});
    auto collector = std::make_shared<CollectOperator>();
    sortOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(reinterpret_cast<int8_t*>(&pipelineContext)));
    sortOperator.setup(ctx);

    for (auto& record : records) {
        sortOperator.execute(ctx, record);
    }

    // Check if the records have been collected in the operator handler state
    auto state = handler->getState();
    ASSERT_EQ(state->getNumberOfEntries(), NUM_RECORDS);

    for (size_t i = 0; i < NUM_RECORDS; i++) {
        auto entry = state->getEntry(i);
        ASSERT_EQ(*(reinterpret_cast<EncodedType*>(entry)), encodeData(static_cast<Type>(i * 10)));// f1 encoded value
        ASSERT_EQ(*(reinterpret_cast<Type*>(entry) + 1), static_cast<Type>(i * 10));               // f1 original value
        ASSERT_EQ(*(reinterpret_cast<Type*>(entry) + 2), static_cast<Type>(i));                    // f2 original value
    }
}

/**
 * @brief Tests if the sort operator collects records for sorting on the second field
 */
TYPED_TEST(BatchSortOperatorTest, SortOperatorOnSecondColumnTest) {
    using Type = typename TypeParam::first_type;
    using EncodedType = typename TypeParam::second_type;
    constexpr size_t NUM_RECORDS = 100;

    std::vector<Record> records;
    createRecords<Type, EncodedType>(records, NUM_RECORDS);

    auto nType = Util::getPhysicalTypePtr<Type>();
    auto handler = std::make_shared<BatchSortOperatorHandler>(std::vector<PhysicalTypePtr>({nType, nType}),
                                                              std::vector<Record::RecordFieldIdentifier>({"f1", "f2"}),
                                                              std::vector<Record::RecordFieldIdentifier>({"f2"}));
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto sortOperator = BatchSort(0, {nType, nType}, {"f1", "f2"}, {"f2"});
    auto collector = std::make_shared<CollectOperator>();
    sortOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(reinterpret_cast<int8_t*>(&pipelineContext)));
    sortOperator.setup(ctx);

    for (auto& record : records) {
        sortOperator.execute(ctx, record);
    }

    // Check if the records have been collected in the operator handler state
    auto state = handler->getState();
    ASSERT_EQ(state->getNumberOfEntries(), NUM_RECORDS);

    for (size_t i = 0; i < NUM_RECORDS; i++) {
        auto entry = state->getEntry(i);
        ASSERT_EQ(*(reinterpret_cast<EncodedType*>(entry)), encodeData(static_cast<Type>(i)));// f2 encoded value
        ASSERT_EQ(*(reinterpret_cast<Type*>(entry) + 1), static_cast<Type>(i * 10));          // f1 original value
        ASSERT_EQ(*(reinterpret_cast<Type*>(entry) + 2), static_cast<Type>(i));               // f2 original value
    }
}

/**
 * @brief Tests if the sort operator collects records over multiple pages
 */
TYPED_TEST(BatchSortOperatorTest, SortOperatorMuliplePagesTest) {
    using Type = typename TypeParam::first_type;
    using EncodedType = typename TypeParam::second_type;
    constexpr auto NUM_RECORDS = 1025;// 1025 * 2 (Fields) * 2 Bytes = 4100 Bytes > 4096 Bytes (Page Size)

    std::vector<Record> records;
    createRecords<Type, EncodedType>(records, NUM_RECORDS);
    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
    auto handler = std::make_shared<BatchSortOperatorHandler>(std::vector<PhysicalTypePtr>({integerType, integerType}),
                                                              std::vector<Record::RecordFieldIdentifier>({"f1", "f2"}),
                                                              std::vector<Record::RecordFieldIdentifier>({"f1"}));
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto sortOperator = BatchSort(0, {integerType, integerType}, {"f1", "f2"}, {"f1"});
    auto collector = std::make_shared<CollectOperator>();
    sortOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    sortOperator.setup(ctx);

    for (auto record : records) {
        sortOperator.execute(ctx, record);
    }

    auto state = handler->getState();
    ASSERT_EQ(state->getNumberOfEntries(), NUM_RECORDS);
}
}// namespace NES::Runtime::Execution::Operators