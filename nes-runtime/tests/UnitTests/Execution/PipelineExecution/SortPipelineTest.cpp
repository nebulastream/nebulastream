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
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Sort/Sort.hpp>
#include <Execution/Operators/Relational/Sort/SortScan.hpp>
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

class SortPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {
  public:
    Nautilus::CompilationOptions options;
    ExecutablePipelineProvider* provider{};
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SortPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SortPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup SortPipelineTest test case.");
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
    static void TearDownTestCase() { NES_INFO("Tear down SortPipelineTest test class."); }
};

/**
 * @brief Emit operator that emits a row oriented tuple buffer.
 */
TEST_P(SortPipelineTest, SortPipelineTest) {
    auto numberOfTuples = 100;
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT32);
    schema->addField("f2", BasicType::INT32);

    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto sortOperator = std::make_shared<Operators::Sort>(s);
    auto sortScanOperator = std::make_shared<Operators::SortScan>(s);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    sortOperator->setChild(sortScanOperator);
    sortScanOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(sortOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = numberOfTuples; i > 0; i--) {
        dynamicBuffer[i]["f1"].write((int32_t) i);
        dynamicBuffer[i]["f2"].write((int32_t) i);
    }
    dynamicBuffer.setNumberOfTuples(numberOfTuples);

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
        ASSERT_EQ(resultDynamicBuffer[i]["f1"].read<int32_t>(), (int32_t) i);
        ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int32_t>(), (int32_t) i);
    }
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        SortPipelineTest,
                        ::testing::Values("PipelineInterpreter", "BCInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<SortPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution