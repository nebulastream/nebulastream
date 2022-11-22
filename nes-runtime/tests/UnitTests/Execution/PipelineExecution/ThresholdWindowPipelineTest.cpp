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
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindowOperatorHandler.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {
class ThresholdWindowPipelineTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ThresholdWindowPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ThresholdWindowPipelineTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup ThresholdWindowPipelineTest test case." << std::endl;
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down ThresholdWindowPipelineTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ThresholdWindowPipelineTest test class." << std::endl; }
};

/**
 * @brief Selection pipeline that execute a select operator and filters elements.
 */
TEST_P(ThresholdWindowPipelineTest, selectionPipeline) {
    auto scanSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    scanSchema->addField("f1", BasicType::INT64);
    scanSchema->addField("f2", BasicType::INT64);
    auto scanMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(scanSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(scanMemoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto one = std::make_shared<Expressions::ConstantIntegerExpression>(1);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, one);
    auto thresholdWindowOperator = std::make_shared<Operators::ThresholdWindow>(greaterThanExpression, readF2, 0);
    scanOperator->setChild(thresholdWindowOperator);

    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema->addField("sum", BasicType::INT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(emitMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    thresholdWindowOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(scanMemoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0]["f1"].write((int64_t) 1);// does not qualify
    dynamicBuffer[0]["f2"].write((int64_t) 10);
    dynamicBuffer[1]["f1"].write((int64_t) 2);// qualifies
    dynamicBuffer[1]["f2"].write((int64_t) 20);
    dynamicBuffer[2]["f1"].write((int64_t) 3);// qualifies
    dynamicBuffer[2]["f2"].write((int64_t) 30);

    // the last tuple closes the window
    dynamicBuffer[3]["f1"].write((int64_t) 1);// does not qualify
    dynamicBuffer[3]["f2"].write((int64_t) 40);
    dynamicBuffer.setNumberOfTuples(4);

    auto executablePipeline = provider->create(pipeline);

    auto handler = std::make_shared<Operators::ThresholdWindowOperatorHandler>();

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    EXPECT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 1);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, resultBuffer);
    EXPECT_EQ(resultDynamicBuffer[0]["sum"].read<int64_t>(), 50);
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        ThresholdWindowPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<ThresholdWindowPipelineTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Runtime::Execution
