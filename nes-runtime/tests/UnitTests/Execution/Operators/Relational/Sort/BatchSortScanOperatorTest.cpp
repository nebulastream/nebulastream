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
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Sort/BatchSortOperatorHandler.hpp>
#include <Execution/Operators/Relational/Sort/BatchSortScan.hpp>
#include <Execution/RecordBuffer.hpp>
#include <NesBaseTest.hpp>
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
class BatchSortScanOperatorTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("BatchSortScanOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO2("Setup BatchSortScanOperatorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { Testing::NESBaseTest::SetUp(); }
    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO2("Tear down BatchSortScanOperatorTest test class."); }
};

// Fills the state of the sort operator with values in descending order
template<typename TypeParam>
void fillState(uint64_t numberOfRecord, std::shared_ptr<BatchSortOperatorHandler> handler) {
    auto state = handler->getState();
    auto entrySize = sizeof(TypeParam);
    auto stateRef = Nautilus::Interface::PagedVectorRef(Value<MemRef>((int8_t*) state), entrySize);
    std::mt19937 randomGenerator(42);
    for (size_t j = 0; j < numberOfRecord; j++) {
        Value<> val((TypeParam) (randomGenerator()));
        auto entry = stateRef.allocateEntry();
        entry.store(val);
    }
}

using TestTypes = ::testing::Types<uint32_t, int32_t, uint64_t, int64_t, uint16_t, int16_t, uint8_t, int8_t>;
TYPED_TEST_SUITE(BatchSortScanOperatorTest, TestTypes);

/**
 * @brief Tests if the sort operator collects records with multiple fields
 */
TYPED_TEST(BatchSortScanOperatorTest, SortOperatorMultipleFieldsTest) {
    using cType = typename TypeParam::first_type;
    using nType = typename TypeParam::second_type;

    std::shared_ptr<BufferManager> bm = std::make_shared<BufferManager>();
    std::shared_ptr<WorkerContext> wc = std::make_shared<WorkerContext>(0, bm, 100);

    auto numberOfRecords = 100;
    auto entrySize = sizeof(cType);
    auto handler = std::make_shared<BatchSortOperatorHandler>(entrySize);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    handler->setup(pipelineContext);
    fillState<cType>(numberOfRecords, handler);

    auto type = Util::getPhysicalTypePtr<cType>();
    auto dataTypes = std::vector<PhysicalTypePtr>{type};
    std::vector<Record::RecordFieldIdentifier> fieldNames = {"f1"};

    auto sortScanOperator = BatchSortScan(0, fieldNames, dataTypes, {0});
    auto collector = std::make_shared<CollectOperator>();
    sortScanOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    sortScanOperator.setup(ctx);
    auto tupleBuffer = bm->getBufferBlocking();
    auto record = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
    sortScanOperator.open(ctx, record);

    ASSERT_EQ(collector->records.size(), 100);
    ASSERT_EQ(collector->records[0].numberOfFields(), 1);

    // Records should be in ascending order
    auto prev = collector->records[0].read("f1").as<nType>();
    for (int i = 1; i < numberOfRecords; i++){
        auto cur = collector->records[i].read("f1").as<nType>();
        std::cout << prev << " " << cur << std::endl;
        ASSERT_LE(prev, cur);
        prev = cur;
    }
}

}// namespace NES::Runtime::Execution::Operators