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
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class PipelineExecutionTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("PipelineExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup PipelineExecutionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup PipelineExecutionTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down PipelineExecutionTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down PipelineExecutionTest test class." << std::endl; }
};

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext()
        : PipelineExecutionContext(
            -1,// mock pipeline id
            0, // mock query id
            nullptr,
            [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                this->buffers.emplace_back(std::move(buffer));
            },
            [this](TupleBuffer& buffer) {
                this->buffers.emplace_back(std::move(buffer));
            },
            {}){
            // nop
        };

    std::vector<TupleBuffer> buffers;
};

/**
 * @brief Emit operator that emits a row oriented tuple buffer.
 */
TEST_F(PipelineExecutionTest, scanEmitPipeline) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<WorkerContext>(0, bm, 100);
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto pipelineContext = MockedPipelineExecutionContext();

    auto scanOperator = std::make_shared<Scan>(memoryLayout);
    auto emitOperator = std::make_shared<Emit>(memoryLayout);
    scanOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator.get());

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < dynamicBuffer.getCapacity(); i++) {
        dynamicBuffer[i]["f1"].write((int64_t) i % 2);
        dynamicBuffer[i]["f2"].write((int64_t) 1);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = NautilusExecutablePipelineStage(pipeline);

    executablePipeline.setup(pipelineContext);
    executablePipeline.execute(buffer, pipelineContext, *wc);
    executablePipeline.stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
}


}// namespace NES::Runtime::Execution::Operators