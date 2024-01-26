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
#include <BaseIntegrationTest.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {

class SequenceNumberPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;
    Nautilus::CompilationOptions options;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SequenceNumberPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SequenceNumberPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup SequenceNumberPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SequenceNumberPipelineTest test class."); }
};

/**
 * @brief This method creates four buffers and sets the f1 = 10 for all tuples in the second and fourth buffer
 */
std::vector<TupleBuffer> createDataAllSeqNumbersEmitted(BufferManagerPtr bm, SchemaPtr schema) {
    std::vector<TupleBuffer> retBuffers;
    constexpr uint64_t NUM_BUF = 4;

    for (uint64_t bufCnt = 0; bufCnt < NUM_BUF; ++bufCnt) {
        auto buffer = bm->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(buffer, schema);
        for (int64_t i = 0; i < 100; ++i) {
            dynamicBuffer[i]["f1"].write(i % 10_s64);
            dynamicBuffer[i]["f2"].write(+1_s64);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }

        if (bufCnt == 1 || bufCnt == 3) {
            for (uint64_t i = 0; i < dynamicBuffer.getCapacity(); ++i) {
                dynamicBuffer[i]["f1"].write(+10_s64);
            }
        }
        buffer.setSequenceNumber(bufCnt + 1);
        retBuffers.emplace_back(buffer);
    }

    return retBuffers;
}

/**
 * @brief Selection pipeline that execute a select operator and filters all tuples that are not satisfying (f1 == 5)
 * We send four buffers and create the data so that no tuple satisfies the condition for the second, and fourth buffer.
 * Then we check if the PipelineExecutionContext has seen all sequence numbers, as we have to pass on all sequence numbers
 */
TEST_P(SequenceNumberPipelineTest, testAllSequenceNumbersGetEmitted) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ConstantInt64ValueExpression>(5);
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto equalsExpression = std::make_shared<Expressions::EqualsExpression>(readF1, readF2);
    auto selectionOperator = std::make_shared<Operators::Selection>(equalsExpression);
    scanOperator->setChild(selectionOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    selectionOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);
    auto executablePipeline = provider->create(pipeline, options);

    auto pipelineContext = MockedPipelineExecutionContext();
    executablePipeline->setup(pipelineContext);
    for (auto& buf : createDataAllSeqNumbersEmitted(bm, schema)) {
        executablePipeline->execute(buf, pipelineContext, *wc);
    }
    executablePipeline->stop(pipelineContext);

    // Checking the output
    ASSERT_EQ(pipelineContext.buffers.size(), 4);
    for (const auto& buf : pipelineContext.buffers) {
        auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buf);
        for (uint64_t i = 0; i < resultDynamicBuffer.getNumberOfTuples(); i++) {
            ASSERT_EQ(resultDynamicBuffer[i]["f1"].read<int64_t>(), 5);
            ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int64_t>(), 1);
        }
    }

    // Checking, if we have seen all sequence numbers
    ASSERT_THAT(pipelineContext.seenSeqNumbers, ::testing::UnorderedElementsAreArray({1, 2, 3, 4}));
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        SequenceNumberPipelineTest,
                        ::testing::Values("PipelineInterpreter", "BCInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<SequenceNumberPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution