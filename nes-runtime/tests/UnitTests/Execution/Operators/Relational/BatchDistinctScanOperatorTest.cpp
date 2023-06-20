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
#include <Execution/Operators/Relational/BatchDistinctOperatorHandler.hpp>
#include <Execution/Operators/Relational/BatchDistinctScan.hpp>
#include <Execution/RecordBuffer.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

template<typename T>
class BatchDistinctScanOperatorTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("BatchDistinctScanOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO2("Setup BatchDistinctScanOperatorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { Testing::NESBaseTest::SetUp(); }
    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO2("Tear down BatchDistinctScanOperatorTest test class."); }
};

// Fills the state of the sort operator with values in descending order
template<typename TypeParam>
void fillState(uint64_t numRecords, uint64_t numDistinctRecords, std::shared_ptr<BatchDistinctOperatorHandler> handler) {
    auto state = handler->getState();
    auto entrySize = sizeof(TypeParam);
    auto stateRef = Nautilus::Interface::PagedVectorRef(Value<MemRef>((int8_t*) state), entrySize);
    for (uint32_t j = 0; j < numRecords; j++) {
        // Todo fill record with data fitting for distinct
        Value<> val((TypeParam) (j % numDistinctRecords));
        auto entry = stateRef.allocateEntry();
        entry.store(val);
    }
}

using TestTypes = ::testing::Types<uint32_t>;
// using TestTypes = ::testing::Types<uint32_t, int32_t, uint64_t, int64_t, uint16_t, int16_t, uint8_t, int8_t>;
TYPED_TEST_SUITE(BatchDistinctScanOperatorTest, TestTypes);

/**
 * @brief Tests if the sort operator collects records with multiple fields
 */
TYPED_TEST(BatchDistinctScanOperatorTest, SortOperatorMultipleFieldsTest) {
    // Todo: Create a test that confirms the correct behavior of DISTINCT
    std::shared_ptr<BufferManager> bm = std::make_shared<BufferManager>();
    std::shared_ptr<WorkerContext> wc = std::make_shared<WorkerContext>(0, bm, 100);

    uint64_t numRecords = 100;
    uint64_t numDistinctRecords = 3;
    auto entrySize = sizeof(TypeParam);
    auto handler = std::make_shared<BatchDistinctOperatorHandler>(entrySize);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    handler->setup(pipelineContext);
    //Todo: fillState needs to be DISTINCT specific
    fillState<TypeParam>(numRecords, numDistinctRecords , handler);

    auto type = Util::getPhysicalTypePtr<TypeParam>();
    auto dataTypes = std::vector<PhysicalTypePtr>{type};
    std::vector<Record::RecordFieldIdentifier> fieldNames = {"f1"};

    // 
    auto distinctScanOperator = BatchDistinctScan(0, fieldNames, dataTypes);
    auto collector = std::make_shared<CollectOperator>();
    distinctScanOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    //Todo: This needs to work out
    distinctScanOperator.setup(ctx);
    auto tupleBuffer = bm->getBufferBlocking();
    auto record = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
    distinctScanOperator.open(ctx, record);

    // Only three distinct values should be returned by distinct
    ASSERT_EQ(collector->records.size(), 3);
    // There should still only be one field
    ASSERT_EQ(collector->records[0].numberOfFields(), 1);
    // Records should be in ascending order
    for (int i = 0; i < 3; i++) {
        ASSERT_EQ(collector->records[i].read("f1"), i);
    }
}

}// namespace NES::Runtime::Execution::Operators