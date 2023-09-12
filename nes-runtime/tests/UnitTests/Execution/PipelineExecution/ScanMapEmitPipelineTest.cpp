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
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
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

class ScanMapEmitPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {
  public:
    Nautilus::CompilationOptions options;
    ExecutablePipelineProvider* provider{};
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ScanMapEmitPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ScanMapEmitPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup ScanMapEmitPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        options.setDumpToConsole(true);
        options.setDumpToFile(true);
        options.setCUDASdkPath("/usr/local/cuda/");
        options.useCUDA(true);
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ScanMapEmitPipelineTest test class."); }
};

/**
* @brief Emit operator that emits a row oriented tuple buffer.
*/
TEST_P(ScanMapEmitPipelineTest, scanEmitMapPipeline) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT32);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readConstInt5 = std::make_shared<Expressions::ConstantInt64ValueExpression>(5);
    auto addExpression = std::make_shared<Expressions::AddExpression>(readF1, readConstInt5);
    auto overrideF1Expression = std::make_shared<Expressions::WriteFieldExpression>("f1", addExpression);
    auto mapOperator = std::make_shared<Operators::Map>(overrideF1Expression);
    scanOperator->setChild(mapOperator);

    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < dynamicBuffer.getCapacity(); i++) {
        dynamicBuffer[i]["f1"].write((int32_t) i);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = provider->create(pipeline, options);

    auto pipelineContext = MockedPipelineExecutionContext();
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), memoryLayout->getCapacity());

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < memoryLayout->getCapacity(); i++) {
        ASSERT_EQ(resultDynamicBuffer[i]["f1"].read<int32_t>(), (int32_t) i + 5);
    }
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        ScanMapEmitPipelineTest,
                        ::testing::Values("PipelineCompiler"),
                        [](const testing::TestParamInfo<ScanMapEmitPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution