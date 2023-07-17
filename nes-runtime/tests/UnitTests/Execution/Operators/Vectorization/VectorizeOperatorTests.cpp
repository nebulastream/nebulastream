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

#include <API/Schema.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Experimental/Vectorization/StagingHandler.hpp>
#include <Execution/Operators/Experimental/Vectorization/Vectorize.hpp>
#include <Execution/Operators/Experimental/Vectorization/VectorizableOperator.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Util/Logger/Logger.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <NesBaseTest.hpp>
#include <gtest/gtest.h>

namespace NES::Runtime::Execution::Operators {

class VectorizeOperatorTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("VectorizeOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup VectorizeOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down VectorizeOperatorTest test class."); }
};

class VectorizedCollectOperator : public VectorizableOperator {
public:
    VectorizedCollectOperator(std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider, std::vector<Record::RecordFieldIdentifier> projections)
        : memoryProvider(std::move(memoryProvider))
        , projections(projections)
        , records()
        , invocations(0)
    {

    }

    void execute(ExecutionContext&, RecordBuffer& recordBuffer) const override {
        auto numberOfRecords = recordBuffer.getNumRecords();
        auto bufferAddress = recordBuffer.getBuffer();
        for (Value<UInt64> i = (uint64_t) 0; i < numberOfRecords; i = i + (uint64_t) 1) {
            auto record = memoryProvider->read(projections, bufferAddress, i);
            records.push_back(record);
        }
        invocations = invocations + 1;
    };

    std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider;
    std::vector<Record::RecordFieldIdentifier> projections;
    mutable std::vector<Record> records;
    mutable uint64_t invocations;
};

/**
 * @brief Vectorize operator that processes tuple buffers.
 */
TEST_F(VectorizeOperatorTest, vectorizeTupleBuffer) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    auto dynamicBufferCapacity = dynamicBuffer.getCapacity();
    for (uint64_t i = 0; i < dynamicBufferCapacity; i++) {
        dynamicBuffer[i]["f1"].write((int64_t) i % 2);
        dynamicBuffer[i]["f2"].write((int64_t) i);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    auto stageBuffer = bm->getBufferBlocking();
    auto stageBufferAddress = Value<MemRef>((int8_t*) std::addressof(stageBuffer));
    auto stageBufferMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    // Set the stage buffer capacity to half of the tuple buffer's capacity. Hence, the number of invocations should be two.
    auto stageBufferCapacity = dynamicBufferCapacity / 2;
    auto handler = std::make_shared<StagingHandler>(std::move(stageBufferMemoryProviderPtr), stageBufferAddress, stageBufferCapacity);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    std::vector<Record::RecordFieldIdentifier> projections = {"f1", "f2"};
    auto collectMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto collectOperator = std::make_shared<VectorizedCollectOperator>(std::move(collectMemoryProviderPtr), projections);
    auto vectorizeOperator = Vectorize(pipelineContext.getOperatorHandlers().size() - 1);
    vectorizeOperator.setChild(collectOperator);

    auto bufferRef = Value<MemRef>((int8_t*) std::addressof(buffer));
    RecordBuffer recordBuffer = RecordBuffer(bufferRef);
    auto bufferAddress = recordBuffer.getBuffer();
    auto numberOfRecords = recordBuffer.getNumRecords();
    auto memoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    for (Value<UInt64> i = (uint64_t) 0; i < numberOfRecords; i = i + (uint64_t) 1) {
        auto record = memoryProviderPtr->read(projections, bufferAddress, i);
        vectorizeOperator.execute(ctx, record);
    }

    ASSERT_EQ(collectOperator->records.size(), numberOfRecords);
    ASSERT_EQ(collectOperator->invocations, dynamicBufferCapacity / stageBufferCapacity);
    for (uint64_t i = 0; i < collectOperator->records.size(); i++) {
        auto& record = collectOperator->records[i];
        ASSERT_EQ(record.numberOfFields(), 2);
        ASSERT_EQ(record.read("f1"), (int64_t) i % 2);
        ASSERT_EQ(record.read("f2"), (int64_t) i);
    }
}

} // namespace NES::Runtime::Execution::Operators
