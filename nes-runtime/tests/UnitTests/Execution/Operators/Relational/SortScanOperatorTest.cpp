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
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Sort/SortScan.hpp>
#include <Execution/Operators/Relational/Sort/SortOperatorHandler.hpp>
#include <Nautilus/Interface/Stack/Stack.hpp>
#include <Execution/RecordBuffer.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class SortScanOperatorTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SortScanOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SortScanOperatorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        bm = std::make_shared<BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }
    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SortScanOperatorTest test class."); }
};

// Fills the state of the sort operator with values in descending order
void fillState(uint64_t numberOfRecord, std::shared_ptr<SortOperatorHandler> handler) {
    auto state = handler->getState();
    auto entrySize = sizeof(int32_t);
    auto stateRef = Nautilus::Interface::StackRef(Value<MemRef>((int8_t*) state), entrySize);
    for (size_t j = 0; j < numberOfRecord; j++) {
        Value<Int32> val((int32_t) (numberOfRecord - j));
        auto entry = stateRef.allocateEntry();
        entry.store(val);
    }
}

/**
 * @brief Tests if the sort operator collects records with multiple fields
 */
TEST_F(SortScanOperatorTest, SortOperatorMultipleFieldsTest) {
    auto numberOfRecords = 100;
    auto entrySize = sizeof(int32_t);
    auto handler = std::make_shared<SortOperatorHandler>(entrySize);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    handler->setup(pipelineContext);
    fillState(numberOfRecords, handler);

    DefaultPhysicalTypeFactory physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto integerType = physicalDataTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
    auto dataTypes = std::vector<PhysicalTypePtr>{integerType};
    std::vector<Record::RecordFieldIdentifier> fieldNames = {"f1"};

    auto sortScanOperator = SortScan(0, fieldNames, dataTypes);
    auto collector = std::make_shared<CollectOperator>();
    sortScanOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    sortScanOperator.setup(ctx);
    auto tupleBuffer = bm->getBufferBlocking();
    auto record = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
    for (int i = 0; i < numberOfRecords; i++){
        sortScanOperator.open(ctx, record);
    }

    ASSERT_EQ(collector->records.size(), 100);
    ASSERT_EQ(collector->records[0].numberOfFields(), 1);
    // Records should be in ascending order
    for (int i = 0; i < numberOfRecords; i++){
        ASSERT_EQ(collector->records[i].read("f1"), i+1);
    }
}

}// namespace NES::Runtime::Execution::Operators