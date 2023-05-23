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
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/OutOfOrderRatio/OutOfOrderRatioOperator.hpp>
#include <Execution/Operators/OutOfOrderRatio/OutOfOrderRatioOperatorHandler.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Execution/StatisticsCollector/BranchMisses.hpp>
#include <Execution/StatisticsCollector/CacheMisses.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/Adwin/Adwin.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/ChangeDetectorWrapper.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/SeqDrift2/SeqDrift2.hpp>
#include <Execution/StatisticsCollector/OutOfOrderRatio.hpp>
#include <Execution/StatisticsCollector/PipelineRuntime.hpp>
#include <Execution/StatisticsCollector/PipelineSelectivity.hpp>
#include <Execution/StatisticsCollector/Profiler.hpp>
#include <Execution/StatisticsCollector/StatisticsCollector.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <random>

namespace NES::Runtime::Execution {

class MultiplePipelinesTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider{};
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MultiplePipelinesTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup MultiplePipelinesTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup MultiplePipelinesTest test case." << std::endl;
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down MultiplePipelinesTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down MultiplePipelinesTest test class." << std::endl; }
};

/**
* @brief test statistics collection with multiple pipelines
*/
TEST_P(MultiplePipelinesTest, multiplePipelines) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // create pipeline
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(2);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
    auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
    scanOperator->setChild(selectionOperator);

    auto constantInt2 = std::make_shared<Expressions::ConstantIntegerExpression>(9);
    auto lessThanExpression = std::make_shared<Expressions::LessThanExpression>(readField1, constantInt2);
    auto selectionOperator2 = std::make_shared<Operators::Selection>(lessThanExpression);
    selectionOperator->setChild(selectionOperator2);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    selectionOperator2->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    // create pipeline 2
    auto scanMemoryProviderPtrP2 = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperatorP2 = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtrP2));

    auto constantIntP2 = std::make_shared<Expressions::ConstantIntegerExpression>(5);
    auto greaterThanExpressionP2 = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantIntP2);
    auto selectionOperatorP2 = std::make_shared<Operators::Selection>(greaterThanExpressionP2);
    scanOperatorP2->setChild(selectionOperatorP2);

    auto constantInt2P2 = std::make_shared<Expressions::ConstantIntegerExpression>(8);
    auto lessThanExpressionP2 = std::make_shared<Expressions::LessThanExpression>(readField1, constantInt2P2);
    auto selectionOperator2P2 = std::make_shared<Operators::Selection>(lessThanExpressionP2);
    selectionOperatorP2->setChild(selectionOperator2P2);

    auto emitMemoryProviderPtrP2 = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperatorP2 = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtrP2));
    selectionOperator2P2->setChild(emitOperatorP2);

    auto pipeline2 = std::make_shared<PhysicalOperatorPipeline>();
    pipeline2->setRootOperator(scanOperatorP2);

    std::vector<TupleBuffer> bufferVector;
    std::vector<std::uniform_int_distribution<int64_t>> distVector;

    // generate values in random order
    std::random_device rd;
    std::mt19937 rng(rd());
    distVector.push_back(std::uniform_int_distribution<int64_t>(2, 8));
    distVector.push_back(std::uniform_int_distribution<int64_t>(0, 10));
    distVector.push_back(std::uniform_int_distribution<int64_t>(8, 10));

    for (int j = 0; j < 3; ++j) {
        auto buffer = bm->getBufferBlocking();
        bufferVector.push_back(buffer);
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t i = 0; i < 500; i++) {
            int64_t randomNumber = distVector[j](rng);
            dynamicBuffer[i]["f1"].write(randomNumber);
            dynamicBuffer[i]["f2"].write((int64_t) 1);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }
    }

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    auto adwin = std::make_unique<Adwin>(0.001, 4);
    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);
    auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(adwin), nautilusExecutablePipelineStage);

    auto pipelineId = 1;

    // pipeline 2
    auto executablePipeline2 = provider->create(pipeline2);
    auto pipelineContext2 = MockedPipelineExecutionContext();

    auto adwinP2 = std::make_unique<Adwin>(0.001, 4);
    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage2 = std::move(executablePipeline2);
    auto nautilusExecutablePipelineStageP2 = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage2);
    auto pipelineSelectivityP2 = std::make_unique<PipelineSelectivity>(std::move(adwinP2), nautilusExecutablePipelineStageP2);

    auto pipelineIdP2 = 2;

    // add statistics of both pipelines to statistics collector
    auto statisticsCollector = std::make_shared<StatisticsCollector>();
    statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));
    statisticsCollector->addStatistic(pipelineIdP2, std::move(pipelineSelectivityP2));

    auto triggerP1 = CollectorTrigger(PipelineStatisticsTrigger,pipelineId);
    auto triggerP2 = CollectorTrigger(PipelineStatisticsTrigger,pipelineIdP2);

    nautilusExecutablePipelineStage->setup(pipelineContext);
    nautilusExecutablePipelineStageP2->setup(pipelineContext2);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        nautilusExecutablePipelineStageP2->execute(buffer, pipelineContext2, *wc);

        statisticsCollector->updateStatisticsHandler(triggerP1);
        statisticsCollector->updateStatisticsHandler(triggerP2);
        //MockedPipelineExecutionContext registers both pipelines with same id, otherwise this would work
        //nautilusExecutablePipelineStage->stop(statisticsCollector);
        //nautilusExecutablePipelineStageP2->stop(statisticsCollector);
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);
    nautilusExecutablePipelineStageP2->stop(pipelineContext2);

    ASSERT_EQ(pipelineContext.buffers.size(), 3);
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        MultiplePipelinesTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<MultiplePipelinesTest::ParamType>& info) {
                            return info.param;
                        });

}