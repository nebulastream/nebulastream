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

class StatisticsCollectorTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider{};
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StatisticsCollectorTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup StatisticsCollectorTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup StatisticsCollectorTest test case." << std::endl;
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down StatisticsCollectorTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down StatisticsCollectorTest test class." << std::endl; }
};

/**
 * @brief test the statistic selectivity
 */
TEST_P(StatisticsCollectorTest, selectivityTest) {
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

    std::vector<TupleBuffer> bufferVector;
    std::vector<std::uniform_int_distribution<int64_t>> distVector;

    // generate values in random order
    std::random_device rd;
    std::mt19937 rng(rd());
    distVector.push_back(std::uniform_int_distribution<int64_t>(2, 8));
    distVector.push_back(std::uniform_int_distribution<int64_t>(0, 10));
    distVector.push_back(std::uniform_int_distribution<int64_t>(8, 10));

    std::vector<double> selectivity(3,0.0);
    uint64_t numberOfTuplesPerBuffer = 500;

    for (int j = 0; j < 3; ++j) {
        auto buffer = bm->getBufferBlocking();
        bufferVector.push_back(buffer);
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        uint64_t tupleCount = 0;
        for (uint64_t i = 0; i < numberOfTuplesPerBuffer; i++) {
            int64_t randomNumber = distVector[j](rng);
            if (randomNumber > 2 && randomNumber < 9){
                tupleCount++;
            }
            dynamicBuffer[i]["f1"].write(randomNumber);
            dynamicBuffer[i]["f2"].write((int64_t) 1);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }
        selectivity[j] = (double) tupleCount / (double) numberOfTuplesPerBuffer;
    }

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    // initialize statistics pipeline selectivity
    auto adwinSelectivity = std::make_unique<Adwin>(0.001, 4);
    auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(adwinSelectivity), nautilusExecutablePipelineStage);

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (size_t i = 0; i < bufferVector.size(); i++) {
        nautilusExecutablePipelineStage->execute(bufferVector[i], pipelineContext, *wc);

        // collect the selectivity
        pipelineSelectivity->collect();
        auto selectivityValue = std::any_cast<double> (pipelineSelectivity->getStatisticValue());
        ASSERT_EQ(selectivity[i], selectivityValue);
        std::cout << "Selectivity: " << selectivityValue << std::endl;
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 3);
    auto resultBuffer = pipelineContext.buffers[0];
    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < resultBuffer.getNumberOfTuples(); i++) {
        ASSERT_GT(resultDynamicBuffer[i]["f1"].read<int64_t>(), 2);
        ASSERT_LT(resultDynamicBuffer[i]["f1"].read<int64_t>(), 9);
        ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int64_t>(), 1);
    }
}

/**
 * @brief test the statistic runtime
 */
TEST_P(StatisticsCollectorTest, runtimeTest) {
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

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    // initialize statistic pipeline runtime
    auto adwinRuntime = std::make_unique<Adwin>(0.001, 4);
    auto pipelineRuntime = std::make_unique<PipelineRuntime>(std::move(adwinRuntime), nautilusExecutablePipelineStage, 5);

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);

        // collect the statistics
        pipelineRuntime->collect();
        auto runtimeValue = std::any_cast<uint64_t>(pipelineRuntime->getStatisticValue());
        ASSERT_GT(runtimeValue, 0);
        std::cout << "Runtime: " << runtimeValue << std::endl;
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 3);
    auto resultBuffer = pipelineContext.buffers[0];
    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < resultBuffer.getNumberOfTuples(); i++) {
        ASSERT_GT(resultDynamicBuffer[i]["f1"].read<int64_t>(), 2);
        ASSERT_LT(resultDynamicBuffer[i]["f1"].read<int64_t>(), 9);
        ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int64_t>(), 1);
    }
}

/**
 * @brief test the statistics branch misses and cache misses
 */
TEST_P(StatisticsCollectorTest, branchAndCacheMissesTest) {
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

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto adwinBranch = std::make_unique<Adwin>(0.001, 4);
    auto adwinCache = std::make_unique<Adwin>(0.001, 4);
    auto profiler = std::make_shared<Profiler>();
    nautilusExecutablePipelineStage->setProfiler(profiler);

    auto branchMisses = std::make_unique<BranchMisses>(std::move(adwinBranch), profiler, 5);
    auto cacheMisses = std::make_unique<CacheMisses>(std::move(adwinCache), profiler, 5);

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        branchMisses->collect();
        auto branchMissesValue = std::any_cast<uint64_t>(branchMisses->getStatisticValue());
        ASSERT_GT(branchMissesValue, 0);
        std::cout << "Branch Misses: " << branchMissesValue << std::endl;
        cacheMisses->collect();
        auto cacheMissesValue = std::any_cast<uint64_t>(cacheMisses->getStatisticValue());
        ASSERT_GT(cacheMissesValue, 0);
        std::cout << "Cache Misses: " << cacheMissesValue << std::endl;
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 3);
    auto resultBuffer = pipelineContext.buffers[0];

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < resultBuffer.getNumberOfTuples(); i++) {
        ASSERT_GT(resultDynamicBuffer[i]["f1"].read<int64_t>(), 2);
        ASSERT_LT(resultDynamicBuffer[i]["f1"].read<int64_t>(), 9);
        ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int64_t>(), 1);
    }
}

/**
 * @brief test out-of-order ratio operator
 */
TEST_P(StatisticsCollectorTest, outOfOrderRatioTest) {
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
    auto handler = std::make_shared<Operators::OutOfOrderRatioOperatorHandler>();

    auto adwin = std::make_unique<Adwin>(0.001, 4);
    auto outOfOrderRatio = std::make_shared<OutOfOrderRatio>(std::move(adwin), handler);

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    outOfOrderRatio->collect();
    auto ratio = std::any_cast<double>(outOfOrderRatio->getStatisticValue());
    ASSERT_GE(ratio, 0.0);
    ASSERT_LE(ratio, 1.0);
    std::cout << "Out-of-order Ratio: " << ratio << std::endl;

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), 100);
}

/**
 * @brief test the trigger of statistics
 */
TEST_P(StatisticsCollectorTest, triggerStatisticsTest) {
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

    auto adwinSelectivity = std::make_unique<Adwin>(0.001, 4);
    auto adwinRuntime = std::make_unique<Adwin>(0.001, 4);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto pipelineId = pipelineContext.getPipelineID();

    auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(adwinSelectivity), nautilusExecutablePipelineStage);
    auto pipelineRuntime = std::make_unique<PipelineRuntime>(std::move(adwinRuntime), nautilusExecutablePipelineStage, 10);

    // create a statistics collector and add the statistics to its list
    auto statisticsCollector = std::make_shared<StatisticsCollector>();
    statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));
    statisticsCollector->addStatistic(pipelineId, std::move(pipelineRuntime));

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        //trigger statistics from nautilus pipeline
        nautilusExecutablePipelineStage->stop(statisticsCollector);
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 3);
    auto resultBuffer = pipelineContext.buffers[0];
    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < resultBuffer.getNumberOfTuples(); i++) {
        ASSERT_GT(resultDynamicBuffer[i]["f1"].read<int64_t>(), 2);
        ASSERT_LT(resultDynamicBuffer[i]["f1"].read<int64_t>(), 9);
        ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int64_t>(), 1);
    }
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        StatisticsCollectorTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<StatisticsCollectorTest::ParamType>& info) {
                            return info.param;
                        });

}