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
#include <Execution/Expressions/ArithmeticalExpressions/MulExpression.hpp>
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/OutOfOrderRatio/OutOfOrderRatioOperator.hpp>
#include <Execution/Operators/OutOfOrderRatio/OutOfOrderRatioOperatorHandler.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Execution/StatisticsCollector/BranchMisses.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/Adwin/Adwin.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/ChangeDetectorWrapper.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/SeqDrift2/SeqDrift2.hpp>
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

class PerformanceTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("PerformanceTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup PerformanceTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup PerformanceTest test case." << std::endl;
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>((8*1024), 262144);
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down PerformanceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down PerformanceTest test class." << std::endl; }
};

/**
* @brief measure performance with statistics collection and Adwin
*/
TEST_P(PerformanceTest, collectionPerformanceTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // generate list of values 0 til 100
    std::vector<int64_t> fieldValues(100);
    std::iota(std::begin(fieldValues), std::end(fieldValues), 1);
    auto rng = std::default_random_engine {};

    std::vector<TupleBuffer> bufferVector;

    for (int i = 0; i < 1000; ++i){
        auto buffer = bm->getBufferBlocking();
        bufferVector.push_back(buffer);
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (int k = 0; k < 5; k++) {
            std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
            for (uint64_t j = 0; j < 100; j++) {
                dynamicBuffer[j]["f1"].write(fieldValues[j]);
                dynamicBuffer[j]["f2"].write((int64_t) 1);
                dynamicBuffer.setNumberOfTuples(j + 1);
            }
        }
    }

    // create pipeline
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(50);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
    auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
    scanOperator->setChild(selectionOperator);

    auto constantInt2 = std::make_shared<Expressions::ConstantIntegerExpression>(2);
    auto addExpression = std::make_shared<Expressions::MulExpression>(readField1, constantInt2);
    auto writeField3 = std::make_shared<Expressions::WriteFieldExpression>("f3", addExpression);
    auto mapOperator = std::make_shared<Operators::Map>(writeField3);
    selectionOperator->setChild(mapOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    auto adwin = std::make_unique<Adwin>(0.001, 5);
    auto changeDetectorWrapper = std::make_unique<ChangeDetectorWrapper>(std::move(adwin));

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto pipelineId = pipelineContext.getPipelineID();

    // initialize statistics pipeline
    auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapper), nautilusExecutablePipelineStage);

    auto statisticsCollector = std::make_shared<StatisticsCollector>();
    statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));

    auto runtimeStart = std::chrono::high_resolution_clock::now();

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        nautilusExecutablePipelineStage->stop(statisticsCollector);
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    auto runtimeEnd = std::chrono::high_resolution_clock::now();
    auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);

    std::cout << "duration with selectivity adwin " << duration.count() << std::endl;

    ASSERT_EQ(pipelineContext.buffers.size(), 1000);
}

/**
* @brief measure performance with statistics collection and SeqDrift2
*/
TEST_P(PerformanceTest, collectionSeqDriftPerformanceTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // generate list of values 0 til 100
    std::vector<int64_t> fieldValues(100);
    std::iota(std::begin(fieldValues), std::end(fieldValues), 1);
    auto rng = std::default_random_engine {};

    std::vector<TupleBuffer> bufferVector;

    for (int i = 0; i < 1000; ++i){
        auto buffer = bm->getBufferBlocking();
        bufferVector.push_back(buffer);
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (int k = 0; k < 5; k++) {
            std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
            for (uint64_t j = 0; j < 100; j++) {
                dynamicBuffer[j]["f1"].write(fieldValues[j]);
                dynamicBuffer[j]["f2"].write((int64_t) 1);
                dynamicBuffer.setNumberOfTuples(j + 1);
            }
        }
    }

    // create pipeline
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(50);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
    auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
    scanOperator->setChild(selectionOperator);

    auto constantInt2 = std::make_shared<Expressions::ConstantIntegerExpression>(2);
    auto addExpression = std::make_shared<Expressions::MulExpression>(readField1, constantInt2);
    auto writeField3 = std::make_shared<Expressions::WriteFieldExpression>("f3", addExpression);
    auto mapOperator = std::make_shared<Operators::Map>(writeField3);
    selectionOperator->setChild(mapOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    auto seqDrift = std::make_unique<SeqDrift2>(0.01, 50);
    auto changeDetectorWrapper = std::make_unique<ChangeDetectorWrapper>(std::move(seqDrift));

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto pipelineId = pipelineContext.getPipelineID();

    // initialize statistics pipeline
    auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapper), nautilusExecutablePipelineStage);

    auto statisticsCollector = std::make_shared<StatisticsCollector>();
    statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));

    auto runtimeStart = std::chrono::high_resolution_clock::now();

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        nautilusExecutablePipelineStage->stop(statisticsCollector);
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    auto runtimeEnd = std::chrono::high_resolution_clock::now();
    auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);

    std::cout << "duration with selectivity seqdrift " << duration.count() << std::endl;

    ASSERT_EQ(pipelineContext.buffers.size(), 1000);
}

/**
* @brief measure performance with branch msses i.e., profiler and normalizer
*/
TEST_P(PerformanceTest, branchMissesTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // generate list of values 0 til 100
    std::vector<int64_t> fieldValues(100);
    std::iota(std::begin(fieldValues), std::end(fieldValues), 1);
    auto rng = std::default_random_engine {};

    std::vector<TupleBuffer> bufferVector;

    for (int i = 0; i < 1000; ++i){
        auto buffer = bm->getBufferBlocking();
        bufferVector.push_back(buffer);
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (int k = 0; k < 5; k++) {
            std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
            for (uint64_t j = 0; j < 100; j++) {
                dynamicBuffer[j]["f1"].write(fieldValues[j]);
                dynamicBuffer[j]["f2"].write((int64_t) 1);
                dynamicBuffer.setNumberOfTuples(j + 1);
            }
        }
    }

    // create pipeline
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(50);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
    auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
    scanOperator->setChild(selectionOperator);

    auto constantInt2 = std::make_shared<Expressions::ConstantIntegerExpression>(2);
    auto addExpression = std::make_shared<Expressions::MulExpression>(readField1, constantInt2);
    auto writeField3 = std::make_shared<Expressions::WriteFieldExpression>("f3", addExpression);
    auto mapOperator = std::make_shared<Operators::Map>(writeField3);
    selectionOperator->setChild(mapOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    auto seqDriftBranchMisses = std::make_unique<SeqDrift2>(0.01, 50);
    auto changeDetectorWrapperBranch = std::make_unique<ChangeDetectorWrapper>(std::move(seqDriftBranchMisses));

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto profiler = std::make_shared<Profiler>();
    nautilusExecutablePipelineStage->setProfiler(profiler);

    auto pipelineId = pipelineContext.getPipelineID();

    // initialize statistics pipeline
    auto branchMisses = std::make_unique<BranchMisses>(std::move(changeDetectorWrapperBranch), profiler, 4);

    auto statisticsCollector = std::make_shared<StatisticsCollector>();
    statisticsCollector->addStatistic(pipelineId, std::move(branchMisses));

    auto runtimeStart = std::chrono::high_resolution_clock::now();

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        nautilusExecutablePipelineStage->stop(statisticsCollector);
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    auto runtimeEnd = std::chrono::high_resolution_clock::now();
    auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);

    std::cout << "duration with branch misses " << duration.count() << std::endl;

    ASSERT_EQ(pipelineContext.buffers.size(), 1000);
}

/**
* @brief measure performance with statistics collection
*/
TEST_P(PerformanceTest, multipleStatisticsTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // generate list of values 0 til 100
    std::vector<int64_t> fieldValues(100);
    std::iota(std::begin(fieldValues), std::end(fieldValues), 1);
    auto rng = std::default_random_engine {};

    std::vector<TupleBuffer> bufferVector;

    for (int i = 0; i < 1000; ++i){
        auto buffer = bm->getBufferBlocking();
        bufferVector.push_back(buffer);
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (int k = 0; k < 5; k++) {
            std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
            for (uint64_t j = 0; j < 100; j++) {
                dynamicBuffer[j]["f1"].write(fieldValues[j]);
                dynamicBuffer[j]["f2"].write((int64_t) 1);
                dynamicBuffer.setNumberOfTuples(j + 1);
            }
        }
    }

    // create pipeline
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(50);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
    auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
    scanOperator->setChild(selectionOperator);

    auto constantInt2 = std::make_shared<Expressions::ConstantIntegerExpression>(2);
    auto addExpression = std::make_shared<Expressions::MulExpression>(readField1, constantInt2);
    auto writeField3 = std::make_shared<Expressions::WriteFieldExpression>("f3", addExpression);
    auto mapOperator = std::make_shared<Operators::Map>(writeField3);
    selectionOperator->setChild(mapOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    auto seqDrift = std::make_unique<SeqDrift2>(0.01, 50);
    auto changeDetectorWrapper = std::make_unique<ChangeDetectorWrapper>(std::move(seqDrift));
    auto seqDriftBranchMisses = std::make_unique<SeqDrift2>(0.01, 50);
    auto changeDetectorWrapperBranch = std::make_unique<ChangeDetectorWrapper>(std::move(seqDriftBranchMisses));

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto profiler = std::make_shared<Profiler>();
    nautilusExecutablePipelineStage->setProfiler(profiler);

    auto pipelineId = pipelineContext.getPipelineID();

    // initialize statistics pipeline
    auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapper), nautilusExecutablePipelineStage);
    auto branchMisses = std::make_unique<BranchMisses>(std::move(changeDetectorWrapperBranch), profiler, 4);

    auto statisticsCollector = std::make_shared<StatisticsCollector>();
    statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));
    statisticsCollector->addStatistic(pipelineId, std::move(branchMisses));

    auto runtimeStart = std::chrono::high_resolution_clock::now();

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        nautilusExecutablePipelineStage->stop(statisticsCollector);
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    auto runtimeEnd = std::chrono::high_resolution_clock::now();
    auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);

    std::cout << "duration with selectitivy and branch misses " << duration.count() << std::endl;

    ASSERT_EQ(pipelineContext.buffers.size(), 1000);
}

/**
* @brief measure performance without statistics collection
*/
TEST_P(PerformanceTest, withoutCollectionPerformanceTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // generate list of values 0 til 100
    std::vector<int64_t> fieldValues(100);
    std::iota(std::begin(fieldValues), std::end(fieldValues), 1);
    auto rng = std::default_random_engine {};

    std::vector<TupleBuffer> bufferVector;

    for (int i = 0; i < 1000; ++i){
        auto buffer = bm->getBufferBlocking();
        bufferVector.push_back(buffer);
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (int k = 0; k < 5; k++) {
            std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
            for (uint64_t j = 0; j < 100; j++) {
                dynamicBuffer[j]["f1"].write(fieldValues[j]);
                dynamicBuffer[j]["f2"].write((int64_t) 1);
                dynamicBuffer.setNumberOfTuples(j + 1);
            }
        }
    }

    // create pipeline
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(50);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
    auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
    scanOperator->setChild(selectionOperator);

    auto constantInt2 = std::make_shared<Expressions::ConstantIntegerExpression>(2);
    auto addExpression = std::make_shared<Expressions::MulExpression>(readField1, constantInt2);
    auto writeField3 = std::make_shared<Expressions::WriteFieldExpression>("f3", addExpression);
    auto mapOperator = std::make_shared<Operators::Map>(writeField3);
    selectionOperator->setChild(mapOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();
    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto runtimeStart = std::chrono::high_resolution_clock::now();

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    auto runtimeEnd = std::chrono::high_resolution_clock::now();
    auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);

    std::cout << "duration " << duration.count() << std::endl;

    ASSERT_EQ(pipelineContext.buffers.size(), 1000);
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        PerformanceTest,
                        ::testing::Values("PipelineCompiler"),
                        [](const testing::TestParamInfo<PerformanceTest::ParamType>& info) {
                            return info.param;
                        });

}