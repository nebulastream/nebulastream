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
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Execution/StatisticsCollector/BranchMisses.hpp>
#include <Execution/StatisticsCollector/CacheMisses.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/Adwin.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/ChangeDetectorWrapper.hpp>
#include <Execution/StatisticsCollector/PipelineRuntime.hpp>
#include <Execution/StatisticsCollector/PipelineSelectivity.hpp>
#include <Execution/StatisticsCollector/Profiler.hpp>
#include <Execution/StatisticsCollector/StatisticsCollector.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <random>

namespace NES::Runtime::Execution {

class StatisticsCollectorTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
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
 * @brief test the statistics selectivity and runtime
 */
TEST_P(StatisticsCollectorTest, collectSelectivityRuntime) {
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

    auto adwinSelectivity = std::make_unique<Adwin>(0.001, 4);
    auto changeDetectorWrapper = std::make_unique<ChangeDetectorWrapper>(std::move(adwinSelectivity));
    auto adwinRuntime = std::make_unique<Adwin>(0.001, 4);
    auto changeDetectorWrapperRuntime = std::make_unique<ChangeDetectorWrapper>(std::move(adwinRuntime));

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto pipelineId = pipelineContext.getPipelineID();

    // initialize statistics pipeline selectivity and pipeline runtime
    auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapper), nautilusExecutablePipelineStage, pipelineId);
    auto pipelineRuntime = std::make_unique<PipelineRuntime>(std::move(changeDetectorWrapperRuntime), nautilusExecutablePipelineStage, pipelineId);

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (TupleBuffer buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);

        // collect the statistics
        pipelineSelectivity->collect();
        pipelineRuntime->collect();
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    // create a statistics collector and add the statistics to its list
    auto statisticsCollector = std::make_unique<StatisticsCollector>();
    statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));
    statisticsCollector->addStatistic(pipelineId, std::move(pipelineRuntime));

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

/**
 * @brief trigger statistics
 */
TEST_P(StatisticsCollectorTest, triggerStatistics) {
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
    auto changeDetectorWrapper = std::make_unique<ChangeDetectorWrapper>(std::move(adwinSelectivity));
    auto adwinRuntime = std::make_unique<Adwin>(0.001, 4);
    auto changeDetectorWrapperRuntime = std::make_unique<ChangeDetectorWrapper>(std::move(adwinRuntime));

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto pipelineId = pipelineContext.getPipelineID();

    auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapper), nautilusExecutablePipelineStage, pipelineId);
    auto pipelineRuntime = std::make_unique<PipelineRuntime>(std::move(changeDetectorWrapperRuntime), nautilusExecutablePipelineStage, pipelineId);

    auto statisticsCollector = std::make_unique<StatisticsCollector>();
    statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));
    statisticsCollector->addStatistic(pipelineId, std::move(pipelineRuntime));

    auto pipelineStatisticsTrigger = CollectorTrigger(Execution::PipelineStatisticsTrigger, pipelineId);

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (TupleBuffer buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        statisticsCollector->updateStatisticsHandler(pipelineStatisticsTrigger);
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
 * @brief test the profiler
 */
TEST_P(StatisticsCollectorTest, testProfiler) {

    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto equalsExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, readF2);
    auto selectionOperator = Operators::Selection(equalsExpression);
    auto collector = std::make_shared<Operators::CollectOperator>();
    selectionOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    std::vector<Record> recordBuffer = std::vector<Record>();

    for (int i = 0; i < 10; ++i) {
        recordBuffer.push_back(Record({{"f1", Value<>(10)}, {"f2", Value<>(5)}}));
    }

    std::vector<perf_hw_id> events;
    events.push_back(PERF_COUNT_HW_BRANCH_MISSES);
    events.push_back(PERF_COUNT_HW_CACHE_MISSES);

    auto profiler = Profiler(events);
    uint64_t branchMissesId = profiler.getEventId(PERF_COUNT_HW_BRANCH_MISSES);
    uint64_t cacheMissesId = profiler.getEventId(PERF_COUNT_HW_CACHE_MISSES);
    const char *fileName = "profilerTestOutput.txt";

    for (Record record : recordBuffer) {
        profiler.startProfiling();

        selectionOperator.execute(ctx, record);

        profiler.stopProfiling();

        ASSERT_NE(profiler.getCount(branchMissesId), 0);
        ASSERT_NE(profiler.getCount(cacheMissesId), 0);
        profiler.writeToOutputFile(fileName);
    }

    ASSERT_EQ(collector->records.size(), 10);

    std::ifstream file(fileName);
    std::string line;
    if (file.is_open()) {
        int64_t count = 0;
        while (std::getline(file, line)) {
            std::cout << line << std::endl;
            ++count;
        }
        file.close();
        remove(fileName);
        ASSERT_EQ(count, events.size() * 10);
    } else {
        NES_THROW_RUNTIME_ERROR("Can not open file");
    }
}

/**
 * @brief test the statistics branch misses and cache misses
 */
TEST_P(StatisticsCollectorTest, collectBranchCacheMisses) {
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
    auto changeDetectorWrapperBranch = std::make_unique<ChangeDetectorWrapper>(std::move(adwinBranch));
    auto adwinCache = std::make_unique<Adwin>(0.001, 4);
    auto changeDetectorWrapperCache = std::make_unique<ChangeDetectorWrapper>(std::move(adwinCache));

    std::vector<perf_hw_id> events;
    events.push_back(PERF_COUNT_HW_BRANCH_MISSES);
    events.push_back(PERF_COUNT_HW_CACHE_MISSES);
    auto profiler = std::make_shared<Profiler>(events);

    auto branchMisses = std::make_unique<BranchMisses>(std::move(changeDetectorWrapperBranch), profiler);
    auto cacheMisses = std::make_unique<CacheMisses>(std::move(changeDetectorWrapperCache), profiler);

    auto statisticsCollector = std::make_unique<StatisticsCollector>();
    //statisticsCollector->addStatistic(std::move(branchMisses));

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (TupleBuffer buffer : bufferVector) {
        branchMisses->startProfiling();
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        branchMisses->collect();
        cacheMisses->collect();
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 3);
    auto resultBuffer = pipelineContext.buffers[0];

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < resultBuffer.getNumberOfTuples(); i++) {
        //ASSERT_EQ(resultDynamicBuffer[i]["f1"].read<int64_t>(), 5);
        //ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int64_t>(), 1);
    }
}

/**
 * @brief test change detection
 */
TEST_P(StatisticsCollectorTest, changeDetection) {
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

    auto adwin = std::make_unique<Adwin>(0.001, 4);
    auto changeDetectorWrapper = std::make_unique<ChangeDetectorWrapper>(std::move(adwin));

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto pipelineId = pipelineContext.getPipelineID();

    auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapper), nautilusExecutablePipelineStage, pipelineId);

    auto statisticsCollector = std::make_unique<StatisticsCollector>();
    statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));

    auto pipelineStatisticsTrigger = CollectorTrigger(Execution::PipelineStatisticsTrigger, pipelineId);

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (TupleBuffer buffer : bufferVector) {
        for (int i = 0; i < 100; ++i) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
            statisticsCollector->updateStatisticsHandler(pipelineStatisticsTrigger);
        }
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 300);
    auto resultBuffer = pipelineContext.buffers[0];
    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < resultBuffer.getNumberOfTuples(); i++) {
        ASSERT_GT(resultDynamicBuffer[i]["f1"].read<int64_t>(), 2);
        ASSERT_LT(resultDynamicBuffer[i]["f1"].read<int64_t>(), 9);
        ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int64_t>(), 1);
    }
}

}