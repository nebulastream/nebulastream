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
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <NesBaseTest.hpp>
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

class ScanEmitPipelineTest : public Testing::TestWithErrorHandling, public AbstractPipelineExecutionTest {
  public:
    Nautilus::CompilationOptions options;
    ExecutablePipelineProvider* provider{};
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ScanEmitPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ScanEmitPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        NES_INFO("Setup ScanEmitPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        options.setDumpToConsole(true);
        options.setDumpToFile(true);
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ScanEmitPipelineTest test class."); }
};

/**
 * @brief Emit operator that emits a row oriented tuple buffer.
 */
TEST_P(ScanEmitPipelineTest, scanEmitPipeline) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT8);
    schema->addField("f2", BasicType::INT16);
    schema->addField("f3", BasicType::INT32);
    schema->addField("f4", BasicType::INT64);
    schema->addField("f5", BasicType::UINT8);
    schema->addField("f6", BasicType::UINT16);
    schema->addField("f7", BasicType::UINT32);
    schema->addField("f8", BasicType::UINT64);
    schema->addField("f9", BasicType::FLOAT32);
    schema->addField("f10", BasicType::FLOAT64);
    schema->addField("f11", BasicType::BOOLEAN);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    scanOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < dynamicBuffer.getCapacity(); i++) {
        dynamicBuffer[i]["f1"].write((int8_t) i);
        dynamicBuffer[i]["f2"].write((int16_t) i);
        dynamicBuffer[i]["f3"].write((int32_t) i);
        dynamicBuffer[i]["f4"].write((int64_t) i);
        dynamicBuffer[i]["f5"].write((uint8_t) i);
        dynamicBuffer[i]["f6"].write((uint16_t) i);
        dynamicBuffer[i]["f7"].write((uint32_t) i);
        dynamicBuffer[i]["f8"].write((uint64_t) i);
        dynamicBuffer[i]["f9"].write((float) 1.1f);
        dynamicBuffer[i]["f10"].write((double) 1.1);
        auto value = (bool) (i % 2);
        dynamicBuffer[i]["f11"].write<bool>(value);
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
        ASSERT_EQ(resultDynamicBuffer[i]["f1"].read<int8_t>(), (int8_t) i);
        ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int16_t>(), (int16_t) i);
        ASSERT_EQ(resultDynamicBuffer[i]["f3"].read<int32_t>(), (int32_t) i);
        ASSERT_EQ(resultDynamicBuffer[i]["f4"].read<int64_t>(), (int64_t) i);
        ASSERT_EQ(resultDynamicBuffer[i]["f5"].read<uint8_t>(), (uint8_t) i);
        ASSERT_EQ(resultDynamicBuffer[i]["f6"].read<uint16_t>(), (uint16_t) i);
        ASSERT_EQ(resultDynamicBuffer[i]["f7"].read<uint32_t>(), (uint32_t) i);
        ASSERT_EQ(resultDynamicBuffer[i]["f8"].read<uint64_t>(), (uint64_t) i);
        ASSERT_EQ(resultDynamicBuffer[i]["f9"].read<float>(), 1.1f);
        ASSERT_EQ(resultDynamicBuffer[i]["f10"].read<double>(), 1.1);
        auto value = (bool) (i % 2);
        ASSERT_EQ(resultDynamicBuffer[i]["f11"].read<bool>(), value);
    }
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        ScanEmitPipelineTest,
                        ::testing::Values("PipelineInterpreter", "BCInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<ScanEmitPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution