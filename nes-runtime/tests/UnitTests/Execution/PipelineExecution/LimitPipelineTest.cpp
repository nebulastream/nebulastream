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
#include <Execution/Operators/Relational/Limit.hpp>
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

class LimitPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {
  public:
    Nautilus::CompilationOptions options;
    ExecutablePipelineProvider* provider{};
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LimitPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LimitPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup LimitPipelineTest test case.");
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
    static void TearDownTestCase() { NES_INFO("Tear down LimitPipelineTest test class."); }
};

/**
 * @brief Limit operator pipeline test
 */
TEST_P(LimitPipelineTest, LimitPipelineTest) {
    constexpr uint64_t LIMIT = 10;
    constexpr uint64_t TUPLES = 20;

    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("f1", BasicType::UINT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));
    auto limitOperator = std::make_shared<Operators::Limit>(0);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    scanOperator->setChild(limitOperator);
    limitOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < TUPLES; ++i) {
        dynamicBuffer[i]["f1"].write((uint64_t) i);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = provider->create(pipeline, options);

    auto handler = std::make_shared<Operators::LimitOperatorHandler>(LIMIT);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), LIMIT);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < LIMIT; ++i) {
        ASSERT_EQ(resultDynamicBuffer[i]["f1"].read<uint64_t>(), (uint64_t) i);
    }
}

INSTANTIATE_TEST_CASE_P(LimitPipelineTest,
                        LimitPipelineTest,
                        ::testing::Values("PipelineInterpreter", "BCInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<LimitPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution