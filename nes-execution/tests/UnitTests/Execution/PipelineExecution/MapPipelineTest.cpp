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
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/SubExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/DivExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/MulExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Streaming/Map.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>

namespace NES::Runtime::Execution
{

/// Dear reviewer, this file is only for local tests as long as the system level test are not merged into the main
/// Therefore, if you can read this please notify me so that I can port this ot the system level tests.
class MapPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest
{
public:
    ExecutablePipelineProvider* provider;
    Memory::BufferManagerPtr bufferManager = Memory::BufferManager::create();
    std::shared_ptr<WorkerContext> wc;
    nautilus::engine::Options options;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("MapPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MapPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup MapPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam()))
        {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        wc = std::make_shared<WorkerContext>(INITIAL<WorkerThreadId>, bufferManager, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down MapPipelineTest test class."); }
};


auto createPipelineForMapWithExpression(Expressions::ExpressionPtr expression, MemoryLayouts::RowLayoutPtr memoryLayout)
{
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowTupleBufferMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto writeExpression = std::make_shared<Expressions::WriteFieldExpression>("f1", expression);
    auto mapOperator = std::make_shared<Operators::Map>(writeExpression);
    scanOperator->setChild(mapOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowTupleBufferMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);
    return pipeline;
}

TEST_P(MapPipelineTest, AddExpression)
{
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ConstantInt64ValueExpression>(5);
    auto addExpression = std::make_shared<Expressions::AddExpression>(readF1, readF2);
    auto pipeline = createPipelineForMapWithExpression(addExpression, memoryLayout);

    auto buffer = bufferManager->getBufferBlocking();
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
    for (int64_t i = 0; i < 100; i++)
    {
        testBuffer[i]["f1"].write(i % 10_s64);
        testBuffer[i]["f2"].write(+1_s64);
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

    auto resulttestBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 100; i++)
    {
        ASSERT_EQ(resulttestBuffer[i]["f1"].read<int64_t>(), (i % 10_s64) + 5);
        ASSERT_EQ(resulttestBuffer[i]["f2"].read<int64_t>(), 1);
    }
}
TEST_P(MapPipelineTest, DivExpression)
{
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ConstantInt64ValueExpression>(5);
    auto addExpression = std::make_shared<Expressions::DivExpression>(readF1, readF2);
    auto pipeline = createPipelineForMapWithExpression(addExpression, memoryLayout);

    auto buffer = bufferManager->getBufferBlocking();
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
    for (int64_t i = 0; i < 100; i++)
    {
        testBuffer[i]["f1"].write(i % 10_s64);
        testBuffer[i]["f2"].write(+1_s64);
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

    auto resulttestBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 100; i++)
    {
        ASSERT_EQ(resulttestBuffer[i]["f1"].read<int64_t>(), (i % 10_s64) / 5);
        ASSERT_EQ(resulttestBuffer[i]["f2"].read<int64_t>(), 1);
    }
}

TEST_P(MapPipelineTest, MulExpression)
{
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ConstantInt64ValueExpression>(5);
    auto addExpression = std::make_shared<Expressions::MulExpression>(readF1, readF2);
    auto pipeline = createPipelineForMapWithExpression(addExpression, memoryLayout);

    auto buffer = bufferManager->getBufferBlocking();
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
    for (int64_t i = 0; i < 100; i++)
    {
        testBuffer[i]["f1"].write(i % 10_s64);
        testBuffer[i]["f2"].write(+1_s64);
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

    auto resulttestBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 100; i++)
    {
        ASSERT_EQ(resulttestBuffer[i]["f1"].read<int64_t>(), (i % 10_s64) * 5);
        ASSERT_EQ(resulttestBuffer[i]["f2"].read<int64_t>(), 1);
    }
}

TEST_P(MapPipelineTest, SubExpression)
{
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ConstantInt64ValueExpression>(5);
    auto addExpression = std::make_shared<Expressions::SubExpression>(readF1, readF2);
    auto pipeline = createPipelineForMapWithExpression(addExpression, memoryLayout);

    auto buffer = bufferManager->getBufferBlocking();
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
    for (int64_t i = 0; i < 100; i++)
    {
        testBuffer[i]["f1"].write(i % 10_s64);
        testBuffer[i]["f2"].write(+1_s64);
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

    auto resulttestBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 100; i++)
    {
        ASSERT_EQ(resulttestBuffer[i]["f1"].read<int64_t>(), (i % 10_s64) - 5);
        ASSERT_EQ(resulttestBuffer[i]["f2"].read<int64_t>(), 1);
    }
}

INSTANTIATE_TEST_CASE_P(
    testIfCompilation,
    MapPipelineTest,
    ::testing::ValuesIn(
        ExecutablePipelineProviderRegistry::getPluginNames().begin(), ExecutablePipelineProviderRegistry::getPluginNames().end()),
    [](const testing::TestParamInfo<MapPipelineTest::ParamType>& info) { return info.param; });

} /// namespace NES::Runtime::Execution
