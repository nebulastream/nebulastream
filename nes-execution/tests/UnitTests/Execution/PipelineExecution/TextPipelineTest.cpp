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

#include <memory>
#include <API/Schema.hpp>
#include <Execution/Functions/LogicalFunctions/EqualsFunction.hpp>
#include <Execution/Functions/ReadFieldFunction.hpp>
#include <Execution/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Streaming/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/ExecutablePipelineProviderRegistry.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES::Runtime::Execution
{

class TextPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest
{
public:
    std::unique_ptr<ExecutablePipelineProvider> provider;
    Memory::BufferManagerPtr bufferManager = Memory::BufferManager::create();
    std::shared_ptr<WorkerContext> wc;
    nautilus::engine::Options options;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("TextPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TextPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup TextPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::instance().contains(GetParam()))
        {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::instance().create(this->GetParam());
        wc = std::make_shared<WorkerContext>(INITIAL<WorkerThreadId>, bufferManager, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TextPipelineTest test class."); }
};

/**
 * @brief Pipeline that execute a text processing function.
 */
TEST_P(TextPipelineTest, textEqualsPipeline)
{
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", DataTypeFactory::createText());
    auto memoryLayout = Memory::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowTupleBufferMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Functions::ReadFieldFunction>("f1");
    auto readF2 = std::make_shared<Functions::ReadFieldFunction>("f1");
    auto equalsFunction = std::make_shared<Functions::EqualsFunction>(readF1, readF2);
    auto selectionOperator = std::make_shared<Operators::Selection>(equalsFunction);
    scanOperator->setChild(selectionOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowTupleBufferMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    selectionOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bufferManager->getBufferBlocking();
    auto testBuffer = Memory::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 100; i++)
    {
        testBuffer[i].writeVarSized("f1", "test", *bufferManager);
        testBuffer.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = provider->create(pipeline, options);

    auto pipelineContext = MockedPipelineExecutionContext({}, false, bufferManager);
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), 100);
    auto resulttestBuffer = Memory::MemoryLayouts::TestTupleBuffer(memoryLayout, resultBuffer);
    for (auto i = 0_u64; i < resultBuffer.getNumberOfTuples(); ++i)
    {
        ASSERT_EQ(resulttestBuffer[i].readVarSized("f1"), "test");
    }
}

INSTANTIATE_TEST_CASE_P(
    testIfCompilation,
    TextPipelineTest,
    ::testing::ValuesIn(ExecutablePipelineProviderRegistry::instance().getRegisteredNames()),
    [](const testing::TestParamInfo<TextPipelineTest::ParamType>& info) { return info.param; });

} /// namespace NES::Runtime::Execution
