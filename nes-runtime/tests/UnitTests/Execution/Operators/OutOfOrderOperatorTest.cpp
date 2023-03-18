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

#include "Execution/Expressions/LogicalExpressions/LessThanExpression.hpp"
#include <API/Schema.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/OutOfOrderRatio/OutOfOrderRatioOperator.hpp>
#include <Execution/Operators/OutOfOrderRatio/OutOfOrderRatioOperatorHandler.hpp>
#include <Execution/Operators/Scan.hpp>
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
#include <random>

namespace NES::Runtime::Execution {

class OutOfOrderOperatorTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("OutOfOrderOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup OutOfOrderOperatorTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup OutOfOrderOperatorTest test case." << std::endl;
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down OutOfOrderOperatorTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down OutOfOrderOperatorTest test class." << std::endl; }
};

/**
 * @brief test out-of-order ratio operator
 */
TEST_P(OutOfOrderOperatorTest, outOfOrderOperatorTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("time", BasicType::UINT64);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto oufOfOrderMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto readTimestamp = std::make_shared<Expressions::ReadFieldExpression>("time");
    auto outOfOrderRatioOperator = std::make_shared<Operators::OutOfOrderRatioOperator>(std::move(readTimestamp),0);
    scanOperator->setChild(outOfOrderRatioOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    outOfOrderRatioOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    std::random_device rd;
    std::mt19937 mt(rd());

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 100; i++) {
        if (i % 10 == 0) {
            std::uniform_int_distribution<uint64_t> dist(999, (1000+i-1));
            auto randomNumber = (uint64_t) dist(mt);
            dynamicBuffer[i]["time"].write((uint64_t) randomNumber);
            dynamicBuffer[i]["f1"].write((int64_t) i % 10);
            dynamicBuffer[i]["f2"].write((int64_t) 1);
            dynamicBuffer.setNumberOfTuples(i + 1);
        } else {
            dynamicBuffer[i]["time"].write((uint64_t) (1000 + i));
            dynamicBuffer[i]["f1"].write((int64_t) i % 10);
            dynamicBuffer[i]["f2"].write((int64_t) 1);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }
    }

    auto executablePipeline = provider->create(pipeline);

    auto record = Record({{"time", Value<>(10)}});
    auto handler = std::make_shared<Operators::OutOfOrderRatioOperatorHandler>(record);

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), 100);
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        OutOfOrderOperatorTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<OutOfOrderOperatorTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Runtime::Execution