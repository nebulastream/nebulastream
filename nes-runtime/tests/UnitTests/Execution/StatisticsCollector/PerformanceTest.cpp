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
#include <Execution/StatisticsCollector/ChangeDetectors/Adwin/Adwin.hpp>
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

class PerformanceTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;
    uint64_t numberOfTuplesInBuffer = 10000; // 100, 1000
    uint64_t filterConstant = 20;
    std::ofstream csvFile;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("PerformanceTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup PerformanceTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup PerformanceTest test case." << std::endl;
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>((1024 * 1024), 8192);
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
TEST_P(PerformanceTest, selectivityAdwinPerformanceTest) {

    csvFile.open(fmt::format("Performance_{}_{}.csv",numberOfTuplesInBuffer,filterConstant), std::ios_base::app);
    csvFile << "ADWIN;Selectivity;";

    for (auto l = 0; l < 10; l++) {
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("f1", BasicType::INT64);
        schema->addField("f2", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        auto rd = std::random_device {};
        auto rng = std::default_random_engine {rd()};
        // generate list of values 1 til 100
        uint64_t fieldValuesSize = 100;
        std::vector<int64_t> fieldValues(fieldValuesSize);
        std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

        // generate tuple buffers
        uint64_t numberOfInputBuffers = 1000;
        std::vector<TupleBuffer> bufferVector;
        for (uint64_t i = 0; i < numberOfInputBuffers; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < numberOfTuplesInBuffer/100; j++) {
                std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
                for (uint64_t k = 0; k < fieldValuesSize; k++) {
                    dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[k]);
                    dynamicBuffer[(j * fieldValuesSize) + k]["f2"].write((int64_t) 1);
                    dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
                }
            }
        }

        // create pipeline
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(filterConstant);
        auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
        auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
        scanOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext();
        auto pipelineId = pipelineContext.getPipelineID();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage =
            std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        // initialize change detector
        auto adwin = std::make_unique<Adwin>(0.6, 5);

        // initialize statistics selectivity and add to statistics collector
        auto pipelineSelectivity =
            std::make_unique<PipelineSelectivity>(std::move(adwin), nautilusExecutablePipelineStage);
        auto statisticsCollector = std::make_shared<StatisticsCollector>();
        statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));

        nautilusExecutablePipelineStage->setup(pipelineContext);
        // measure execution time
        auto runtimeStart = std::chrono::high_resolution_clock::now();
        for (auto& buffer : bufferVector) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
            nautilusExecutablePipelineStage->stop(statisticsCollector);
        }
        auto runtimeEnd = std::chrono::high_resolution_clock::now();
        nautilusExecutablePipelineStage->stop(pipelineContext);

        //auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);
        std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
        csvFile << duration.count() << ";";

        ASSERT_EQ(pipelineContext.buffers.size(), numberOfInputBuffers);
    }
    csvFile << "\n";
    csvFile.close();
}

/**
* @brief measure performance with statistics collection and SeqDrift2
*/
TEST_P(PerformanceTest, selectivitySeqDriftPerformanceTest) {

    csvFile.open(fmt::format("Performance_{}_{}.csv",numberOfTuplesInBuffer,filterConstant), std::ios_base::app);
    csvFile << "SeqDrift;Selectivity;";

    for (auto l = 0; l < 10; l++) {
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("f1", BasicType::INT64);
        schema->addField("f2", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        auto rd = std::random_device {};
        auto rng = std::default_random_engine {rd()};
        // generate list of values 1 til 100
        uint64_t fieldValuesSize = 100;
        std::vector<int64_t> fieldValues(fieldValuesSize);
        std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

        // generate tuple buffers
        uint64_t numberOfInputBuffers = 1000;
        std::vector<TupleBuffer> bufferVector;
        for (uint64_t i = 0; i < numberOfInputBuffers; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < numberOfTuplesInBuffer/100; j++) {
                std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
                for (uint64_t k = 0; k < fieldValuesSize; k++) {
                    dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[k]);
                    dynamicBuffer[(j * fieldValuesSize) + k]["f2"].write((int64_t) 1);
                    dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
                }
            }
        }

        // create pipeline
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(filterConstant);
        auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
        auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
        scanOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext();
        auto pipelineId = pipelineContext.getPipelineID();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage =
            std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        // initialize change detector
        auto seqDrift = std::make_unique<SeqDrift2>(0.1, 200);

        // initialize statistics selectivity and add to statistics collector
        auto pipelineSelectivity =
            std::make_unique<PipelineSelectivity>(std::move(seqDrift), nautilusExecutablePipelineStage);
        auto statisticsCollector = std::make_shared<StatisticsCollector>();
        statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));

        nautilusExecutablePipelineStage->setup(pipelineContext);
        // measure execution time
        auto runtimeStart = std::chrono::high_resolution_clock::now();
        for (auto& buffer : bufferVector) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
            nautilusExecutablePipelineStage->stop(statisticsCollector);
        }
        auto runtimeEnd = std::chrono::high_resolution_clock::now();
        nautilusExecutablePipelineStage->stop(pipelineContext);

        //auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);
        std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
        csvFile << duration.count() << ";";

        ASSERT_EQ(pipelineContext.buffers.size(), numberOfInputBuffers);
    }
    csvFile << "\n";
    csvFile.close();
}

/**
* @brief measure performance with statistics collection and Adwin
*/
TEST_P(PerformanceTest, collectionRuntimePerformanceTest) {

    csvFile.open(fmt::format("Performance_{}_{}.csv",numberOfTuplesInBuffer,filterConstant), std::ios_base::app);
    csvFile << "ADWIN;Runtime;";

    for (auto l = 0; l < 10; l++) {
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("f1", BasicType::INT64);
        schema->addField("f2", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        auto rd = std::random_device {};
        auto rng = std::default_random_engine {rd()};
        // generate list of values 1 til 100
        uint64_t fieldValuesSize = 100;
        std::vector<int64_t> fieldValues(fieldValuesSize);
        std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

        // generate tuple buffers
        uint64_t numberOfInputBuffers = 1000;
        std::vector<TupleBuffer> bufferVector;
        for (uint64_t i = 0; i < numberOfInputBuffers; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < numberOfTuplesInBuffer/100; j++) {
                std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
                for (uint64_t k = 0; k < fieldValuesSize; k++) {
                    dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[k]);
                    dynamicBuffer[(j * fieldValuesSize) + k]["f2"].write((int64_t) 1);
                    dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
                }
            }
        }

        // create pipeline
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(filterConstant);
        auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
        auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
        scanOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext();
        auto pipelineId = pipelineContext.getPipelineID();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage =
            std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        // initialize change detector
        auto adwinRuntime = std::make_unique<Adwin>(0.6, 5);

        // initialize statistic runtime
        auto runtime = std::make_unique<PipelineRuntime>(std::move(adwinRuntime), nautilusExecutablePipelineStage, 4);
        auto statisticsCollector = std::make_shared<StatisticsCollector>();
        statisticsCollector->addStatistic(pipelineId, std::move(runtime));

        nautilusExecutablePipelineStage->setup(pipelineContext);
        auto runtimeStart = std::chrono::high_resolution_clock::now();
        for (auto& buffer : bufferVector) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
            nautilusExecutablePipelineStage->stop(statisticsCollector);
        }
        auto runtimeEnd = std::chrono::high_resolution_clock::now();
        nautilusExecutablePipelineStage->stop(pipelineContext);

        //auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);
        std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
        csvFile << duration.count() << ";";

        ASSERT_EQ(pipelineContext.buffers.size(), numberOfInputBuffers);
    }
    csvFile << "\n";
    csvFile.close();
}

/**
* @brief measure performance with statistics collection and SeqDrift2
*/
TEST_P(PerformanceTest, collectionRuntimeSeqDriftPerformanceTest) {

    csvFile.open(fmt::format("Performance_{}_{}.csv",numberOfTuplesInBuffer,filterConstant), std::ios_base::app);
    csvFile << "SeqDrift;Runtime;";

    for (auto l = 0; l < 10; l++) {
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("f1", BasicType::INT64);
        schema->addField("f2", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        auto rd = std::random_device {};
        auto rng = std::default_random_engine {rd()};
        // generate list of values 1 til 100
        uint64_t fieldValuesSize = 100;
        std::vector<int64_t> fieldValues(fieldValuesSize);
        std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

        // generate tuple buffers
        uint64_t numberOfInputBuffers = 1000;
        std::vector<TupleBuffer> bufferVector;
        for (uint64_t i = 0; i < numberOfInputBuffers; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < numberOfTuplesInBuffer/100; j++) {
                std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
                for (uint64_t k = 0; k < fieldValuesSize; k++) {
                    dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[k]);
                    dynamicBuffer[(j * fieldValuesSize) + k]["f2"].write((int64_t) 1);
                    dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
                }
            }
        }

        // create pipeline
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(filterConstant);
        auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
        auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
        scanOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext();
        auto pipelineId = pipelineContext.getPipelineID();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage =
            std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        // initialize change detector
        auto seqDrift = std::make_unique<SeqDrift2>(0.1, 200);

        // initialize statistic runtime
        auto runtime = std::make_unique<PipelineRuntime>(std::move(seqDrift), nautilusExecutablePipelineStage, 4);
        auto statisticsCollector = std::make_shared<StatisticsCollector>();
        statisticsCollector->addStatistic(pipelineId, std::move(runtime));

        nautilusExecutablePipelineStage->setup(pipelineContext);
        auto runtimeStart = std::chrono::high_resolution_clock::now();
        for (auto& buffer : bufferVector) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
            nautilusExecutablePipelineStage->stop(statisticsCollector);
        }
        auto runtimeEnd = std::chrono::high_resolution_clock::now();
        nautilusExecutablePipelineStage->stop(pipelineContext);

        //auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);
        std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
        csvFile << duration.count() << ";";

        ASSERT_EQ(pipelineContext.buffers.size(), numberOfInputBuffers);
    }
    csvFile << "\n";
    csvFile.close();
}

/**
* @brief measure performance with branch misses i.e., profiler and normalizer
*/
TEST_P(PerformanceTest, branchMissesAdwinTest) {

    csvFile.open(fmt::format("Performance_{}_{}.csv",numberOfTuplesInBuffer,filterConstant), std::ios_base::app);
    csvFile << "ADWIN;BranchMisses;";

    for (auto l = 0; l < 10; l++) {

        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("f1", BasicType::INT64);
        schema->addField("f2", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        auto rd = std::random_device {};
        auto rng = std::default_random_engine {rd()};
        // generate list of values 1 til 100
        uint64_t fieldValuesSize = 100;
        std::vector<int64_t> fieldValues(fieldValuesSize);
        std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

        // generate tuple buffers
        uint64_t numberOfInputBuffers = 1000;
        std::vector<TupleBuffer> bufferVector;
        for (uint64_t i = 0; i < numberOfInputBuffers; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < numberOfTuplesInBuffer/100; j++) {
                std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
                for (uint64_t k = 0; k < fieldValuesSize; k++) {
                    dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[k]);
                    dynamicBuffer[(j * fieldValuesSize) + k]["f2"].write((int64_t) 1);
                    dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
                }
            }
        }

        // create pipeline
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(filterConstant);
        auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
        auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
        scanOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext();
        auto pipelineId = pipelineContext.getPipelineID();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage =
            std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        // initialize change detector
        auto adwin = std::make_unique<Adwin>(0.6, 5);

        // initialize profiler
        auto profiler = std::make_shared<Profiler>();
        nautilusExecutablePipelineStage->setProfiler(profiler);

        // initialize statistics branch misses
        auto branchMisses = std::make_unique<BranchMisses>(std::move(adwin), profiler, 4);
        auto statisticsCollector = std::make_shared<StatisticsCollector>();
        statisticsCollector->addStatistic(pipelineId, std::move(branchMisses));

        nautilusExecutablePipelineStage->setup(pipelineContext);
        auto runtimeStart = std::chrono::high_resolution_clock::now();
        for (auto& buffer : bufferVector) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
            nautilusExecutablePipelineStage->stop(statisticsCollector);
        }
        auto runtimeEnd = std::chrono::high_resolution_clock::now();
        nautilusExecutablePipelineStage->stop(pipelineContext);

        //auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);
        std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
        csvFile << duration.count() << ";";

        ASSERT_EQ(pipelineContext.buffers.size(), numberOfInputBuffers);
    }
    csvFile << "\n";
    csvFile.close();
}

/**
* @brief measure performance with branch misses i.e., profiler and normalizer
*/
TEST_P(PerformanceTest, branchMissesTest) {

    csvFile.open(fmt::format("Performance_{}_{}.csv",numberOfTuplesInBuffer,filterConstant), std::ios_base::app);
    csvFile << "SeqDrift;BranchMisses;";

    for (auto l = 0; l < 10; l++) {

        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("f1", BasicType::INT64);
        schema->addField("f2", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        auto rd = std::random_device {};
        auto rng = std::default_random_engine {rd()};
        // generate list of values 1 til 100
        uint64_t fieldValuesSize = 100;
        std::vector<int64_t> fieldValues(fieldValuesSize);
        std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

        // generate tuple buffers
        uint64_t numberOfInputBuffers = 1000;
        std::vector<TupleBuffer> bufferVector;
        for (uint64_t i = 0; i < numberOfInputBuffers; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < numberOfTuplesInBuffer/100; j++) {
                std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
                for (uint64_t k = 0; k < fieldValuesSize; k++) {
                    dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[k]);
                    dynamicBuffer[(j * fieldValuesSize) + k]["f2"].write((int64_t) 1);
                    dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
                }
            }
        }

        // create pipeline
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(filterConstant);
        auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
        auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
        scanOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext();
        auto pipelineId = pipelineContext.getPipelineID();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage =
            std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        // initialize change detector
        auto seqDriftBranchMisses = std::make_unique<SeqDrift2>(0.1, 200);

        // initialize profiler
        auto profiler = std::make_shared<Profiler>();
        nautilusExecutablePipelineStage->setProfiler(profiler);

        // initialize statistics branch misses
        auto branchMisses = std::make_unique<BranchMisses>(std::move(seqDriftBranchMisses), profiler, 4);
        auto statisticsCollector = std::make_shared<StatisticsCollector>();
        statisticsCollector->addStatistic(pipelineId, std::move(branchMisses));

        nautilusExecutablePipelineStage->setup(pipelineContext);
        auto runtimeStart = std::chrono::high_resolution_clock::now();
        for (auto& buffer : bufferVector) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
            nautilusExecutablePipelineStage->stop(statisticsCollector);
        }
        auto runtimeEnd = std::chrono::high_resolution_clock::now();
        nautilusExecutablePipelineStage->stop(pipelineContext);

        //auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);
        std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
        csvFile << duration.count() << ";";

        ASSERT_EQ(pipelineContext.buffers.size(), numberOfInputBuffers);
    }
    csvFile << "\n";
    csvFile.close();
}

/**
* @brief measure performance with statistics collection out-of-order-ratio and Adwin
*/
TEST_P(PerformanceTest, outOfOrderRatioPerformanceTest) {

    csvFile.open(fmt::format("Performance_{}_{}.csv",numberOfTuplesInBuffer,filterConstant), std::ios_base::app);
    csvFile << "ADWIN;Out-of-order Ratio;";

    for (auto l = 0; l < 10; l++) {
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("timestamp", BasicType::UINT64);
        schema->addField("f1", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        auto rd = std::random_device {};
        auto rng = std::default_random_engine {rd()};
        // generate list of values 1 til 100
        uint64_t fieldValuesSize = 100;
        std::vector<int64_t> fieldValues(fieldValuesSize);
        std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

        std::vector<TupleBuffer> bufferVector;
        uint64_t numberOfInputBuffers = 1000;
        auto timestamp = 0;
        for (uint64_t i = 0; i < numberOfInputBuffers; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < numberOfTuplesInBuffer/100; j++) {
                std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
                for (uint64_t k = 0; k < fieldValuesSize; k++)  {
                    if(((j * fieldValuesSize) + k + 1) % 100 == 0) {
                        // insert out-of-order timestamp
                        dynamicBuffer[(j * fieldValuesSize) + k]["timestamp"].write((uint64_t) timestamp - 3);
                    } else {
                        dynamicBuffer[(j * fieldValuesSize) + k]["timestamp"].write((uint64_t) timestamp);
                        timestamp++;
                    }
                    dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[j]);
                    dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
                }
            }
        }

        // create pipeline
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto oufOfOrderMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto readTimestamp = std::make_shared<Expressions::ReadFieldExpression>("timestamp");
        auto outOfOrderRatioOperator = std::make_shared<Operators::OutOfOrderRatioOperator>(std::move(readTimestamp),0);
        scanOperator->setChild(outOfOrderRatioOperator);

        auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(filterConstant);
        auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
        auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
        outOfOrderRatioOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        // initialize change detector
        auto adwin = std::make_unique<Adwin>(0.6, 5);

        // initialize statistic out-of-order ratio and add to statistics collector
        auto handler = std::make_shared<Operators::OutOfOrderRatioOperatorHandler>();
        auto outOfOrderRatio = std::make_shared<OutOfOrderRatio>(std::move(adwin), handler);
        auto statisticsCollector = std::make_shared<StatisticsCollector>();

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext({handler});
        auto pipelineId = pipelineContext.getPipelineID();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage =
            std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        statisticsCollector->addStatistic(pipelineId, std::move(outOfOrderRatio));

        nautilusExecutablePipelineStage->setup(pipelineContext);
        // measure execution time
        auto runtimeStart = std::chrono::high_resolution_clock::now();
        for (auto& buffer : bufferVector) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
            nautilusExecutablePipelineStage->stop(statisticsCollector);
        }
        auto runtimeEnd = std::chrono::high_resolution_clock::now();
        nautilusExecutablePipelineStage->stop(pipelineContext);

        //auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);
        std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
        csvFile << duration.count() << ";";

        //ASSERT_EQ(pipelineContext.buffers.size(), numberOfInputBuffers);
    }
    csvFile << "\n";
    csvFile.close();
}

/**
* @brief measure performance with statistics collection out-of-orderratio and SeqDrift
*/
TEST_P(PerformanceTest, outOfOrderRatioSeqDriftPerformanceTest) {

    csvFile.open(fmt::format("Performance_{}_{}.csv",numberOfTuplesInBuffer,filterConstant), std::ios_base::app);
    csvFile << "SeqDrift;Out-of-order Ratio;";

    for (auto l = 0; l < 10; l++) {
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("timestamp", BasicType::UINT64);
        schema->addField("f1", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        auto rd = std::random_device {};
        auto rng = std::default_random_engine {rd()};
        // generate list of values 1 til 100
        uint64_t fieldValuesSize = 100;
        std::vector<int64_t> fieldValues(fieldValuesSize);
        std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

        std::vector<TupleBuffer> bufferVector;
        uint64_t numberOfInputBuffers = 1000;
        auto timestamp = 0;
        for (uint64_t i = 0; i < numberOfInputBuffers; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < numberOfTuplesInBuffer/100; j++) {
                std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
                for (uint64_t k = 0; k < fieldValuesSize; k++)  {
                    if(((j * fieldValuesSize) + k + 1) % 100 == 0) {
                        // insert out-of-order timestamp
                        dynamicBuffer[(j * fieldValuesSize) + k]["timestamp"].write((uint64_t) timestamp - 3);
                    } else {
                        dynamicBuffer[(j * fieldValuesSize) + k]["timestamp"].write((uint64_t) timestamp);
                        timestamp++;
                    }
                    dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[j]);
                    dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
                }
            }
        }

        // create pipeline
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto oufOfOrderMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto readTimestamp = std::make_shared<Expressions::ReadFieldExpression>("timestamp");
        auto outOfOrderRatioOperator = std::make_shared<Operators::OutOfOrderRatioOperator>(std::move(readTimestamp),0);
        scanOperator->setChild(outOfOrderRatioOperator);

        auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(filterConstant);
        auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
        auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
        outOfOrderRatioOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        // initialize change detector
        auto seqDrift = std::make_unique<SeqDrift2>(0.1, 200);

        // initialize statistic out-of-order-ratio and add to statistics collector
        auto handler = std::make_shared<Operators::OutOfOrderRatioOperatorHandler>();
        auto outOfOrderRatio = std::make_shared<OutOfOrderRatio>(std::move(seqDrift), handler);
        auto statisticsCollector = std::make_shared<StatisticsCollector>();

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext({handler});
        auto pipelineId = pipelineContext.getPipelineID();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage =
            std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        statisticsCollector->addStatistic(pipelineId, std::move(outOfOrderRatio));

        nautilusExecutablePipelineStage->setup(pipelineContext);
        // measure execution time
        auto runtimeStart = std::chrono::high_resolution_clock::now();
        for (auto& buffer : bufferVector) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
            nautilusExecutablePipelineStage->stop(statisticsCollector);
        }
        auto runtimeEnd = std::chrono::high_resolution_clock::now();
        nautilusExecutablePipelineStage->stop(pipelineContext);

        //auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);
        std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
        csvFile << duration.count() << ";";

        //ASSERT_EQ(pipelineContext.buffers.size(), numberOfInputBuffers);
    }
    csvFile << "\n";
    csvFile.close();
}

/**
* @brief measure performance with statistics collection of selectivity and runtime
*/
TEST_P(PerformanceTest, adwinSelectivityRuntimeTest) {

    csvFile.open(fmt::format("Performance_{}_{}.csv",numberOfTuplesInBuffer,filterConstant), std::ios_base::app);
    csvFile << "ADWIN;Selectivity & Runtime;";

    for (auto l = 0; l < 10; l++) {
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("f1", BasicType::INT64);
        schema->addField("f2", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        auto rd = std::random_device {};
        auto rng = std::default_random_engine {rd()};
        // generate list of values 1 til 100
        uint64_t fieldValuesSize = 100;
        std::vector<int64_t> fieldValues(fieldValuesSize);
        std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

        // generate tuple buffers
        uint64_t numberOfInputBuffers = 1000;
        std::vector<TupleBuffer> bufferVector;
        for (uint64_t i = 0; i < numberOfInputBuffers; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < numberOfTuplesInBuffer/100; j++) {
                std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
                for (uint64_t k = 0; k < fieldValuesSize; k++) {
                    dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[k]);
                    dynamicBuffer[(j * fieldValuesSize) + k]["f2"].write((int64_t) 1);
                    dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
                }
            }
        }

        // create pipeline
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(filterConstant);
        auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
        auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
        scanOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext();
        auto pipelineId = pipelineContext.getPipelineID();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage =
            std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        // initialize change detectors
        auto adwin = std::make_unique<Adwin>(0.6, 5);
        auto adwinRuntime = std::make_unique<Adwin>(0.6, 5);

        // initialize profiler
        auto profiler = std::make_shared<Profiler>();
        nautilusExecutablePipelineStage->setProfiler(profiler);

        // initialize statistics selectivity and runtime
        auto pipelineSelectivity =
            std::make_unique<PipelineSelectivity>(std::move(adwin), nautilusExecutablePipelineStage);
        auto runtime = std::make_unique<PipelineRuntime>(std::move(adwinRuntime), nautilusExecutablePipelineStage, 4);
        auto statisticsCollector = std::make_shared<StatisticsCollector>();
        statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));
        statisticsCollector->addStatistic(pipelineId, std::move(runtime));

        nautilusExecutablePipelineStage->setup(pipelineContext);
        auto runtimeStart = std::chrono::high_resolution_clock::now();
        for (auto& buffer : bufferVector) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
            nautilusExecutablePipelineStage->stop(statisticsCollector);
        }
        auto runtimeEnd = std::chrono::high_resolution_clock::now();
        nautilusExecutablePipelineStage->stop(pipelineContext);

        //auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);
        std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
        csvFile << duration.count() << ";";

        ASSERT_EQ(pipelineContext.buffers.size(), 1000);
    }
    csvFile << "\n";
    csvFile.close();
}

/**
* @brief measure performance with statistics collection of selectivity and runtime
*/
TEST_P(PerformanceTest, selectivityRuntimeTest) {

    csvFile.open(fmt::format("Performance_{}_{}.csv",numberOfTuplesInBuffer,filterConstant), std::ios_base::app);
    csvFile << "SeqDrift;Selectivity & Runtime;";

    for (auto l = 0; l < 10; l++) {
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("f1", BasicType::INT64);
        schema->addField("f2", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        auto rd = std::random_device {};
        auto rng = std::default_random_engine {rd()};
        // generate list of values 1 til 100
        uint64_t fieldValuesSize = 100;
        std::vector<int64_t> fieldValues(fieldValuesSize);
        std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

        // generate tuple buffers
        uint64_t numberOfInputBuffers = 1000;
        std::vector<TupleBuffer> bufferVector;
        for (uint64_t i = 0; i < numberOfInputBuffers; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < numberOfTuplesInBuffer/100; j++) {
                std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
                for (uint64_t k = 0; k < fieldValuesSize; k++) {
                    dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[k]);
                    dynamicBuffer[(j * fieldValuesSize) + k]["f2"].write((int64_t) 1);
                    dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
                }
            }
        }

        // create pipeline
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(filterConstant);
        auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
        auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
        scanOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext();
        auto pipelineId = pipelineContext.getPipelineID();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage =
            std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        // initialize change detectors
        auto seqDrift = std::make_unique<SeqDrift2>(0.1, 200);
        auto seqDriftRuntime = std::make_unique<SeqDrift2>(0.1, 200);

        // initialize profiler
        auto profiler = std::make_shared<Profiler>();
        nautilusExecutablePipelineStage->setProfiler(profiler);

        // initialize statistics selectivity and runtime
        auto pipelineSelectivity =
            std::make_unique<PipelineSelectivity>(std::move(seqDrift), nautilusExecutablePipelineStage);
        auto runtime = std::make_unique<PipelineRuntime>(std::move(seqDriftRuntime), nautilusExecutablePipelineStage, 4);
        auto statisticsCollector = std::make_shared<StatisticsCollector>();
        statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));
        statisticsCollector->addStatistic(pipelineId, std::move(runtime));

        nautilusExecutablePipelineStage->setup(pipelineContext);
        auto runtimeStart = std::chrono::high_resolution_clock::now();
        for (auto& buffer : bufferVector) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
            nautilusExecutablePipelineStage->stop(statisticsCollector);
        }
        auto runtimeEnd = std::chrono::high_resolution_clock::now();
        nautilusExecutablePipelineStage->stop(pipelineContext);

        //auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);
        std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
        csvFile << duration.count() << ";";

        ASSERT_EQ(pipelineContext.buffers.size(), 1000);
    }
    csvFile << "\n";
    csvFile.close();
}

/**
* @brief measure performance with statistics collection
*/
TEST_P(PerformanceTest, adwinSelectivityBranchMissesTest) {

    csvFile.open(fmt::format("Performance_{}_{}.csv",numberOfTuplesInBuffer,filterConstant), std::ios_base::app);
    csvFile << "ADWIN;Selectivity & BranchMisses;";

    for (auto l = 0; l < 10; l++) {
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("f1", BasicType::INT64);
        schema->addField("f2", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        auto rd = std::random_device {};
        auto rng = std::default_random_engine {rd()};
        // generate list of values 1 til 100
        uint64_t fieldValuesSize = 100;
        std::vector<int64_t> fieldValues(fieldValuesSize);
        std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

        // generate tuple buffers
        uint64_t numberOfInputBuffers = 1000;
        std::vector<TupleBuffer> bufferVector;
        for (uint64_t i = 0; i < numberOfInputBuffers; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < numberOfTuplesInBuffer/100; j++) {
                std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
                for (uint64_t k = 0; k < fieldValuesSize; k++) {
                    dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[k]);
                    dynamicBuffer[(j * fieldValuesSize) + k]["f2"].write((int64_t) 1);
                    dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
                }
            }
        }

        // create pipeline
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(filterConstant);
        auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
        auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
        scanOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext();
        auto pipelineId = pipelineContext.getPipelineID();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage =
            std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        // initialize change detectors
        auto adwin = std::make_unique<Adwin>(0.6, 5);
        auto adwinBranchMisses = std::make_unique<Adwin>(0.6, 5);

        // initialize profiler
        auto profiler = std::make_shared<Profiler>();
        nautilusExecutablePipelineStage->setProfiler(profiler);

        // initialize statistics selectivity and branch misses
        auto pipelineSelectivity =
            std::make_unique<PipelineSelectivity>(std::move(adwin), nautilusExecutablePipelineStage);
        auto branchMisses = std::make_unique<BranchMisses>(std::move(adwinBranchMisses), profiler, 4);

        auto statisticsCollector = std::make_shared<StatisticsCollector>();
        statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));
        statisticsCollector->addStatistic(pipelineId, std::move(branchMisses));

        nautilusExecutablePipelineStage->setup(pipelineContext);
        auto runtimeStart = std::chrono::high_resolution_clock::now();
        for (auto& buffer : bufferVector) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
            nautilusExecutablePipelineStage->stop(statisticsCollector);
        }
        auto runtimeEnd = std::chrono::high_resolution_clock::now();
        nautilusExecutablePipelineStage->stop(pipelineContext);

        //auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);
        std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
        csvFile << duration.count() << ";";

        ASSERT_EQ(pipelineContext.buffers.size(), numberOfInputBuffers);
    }
    csvFile << "\n";
    csvFile.close();
}

/**
* @brief measure performance with statistics collection
*/
TEST_P(PerformanceTest, selectivityBranchMissesTest) {

    csvFile.open(fmt::format("Performance_{}_{}.csv",numberOfTuplesInBuffer,filterConstant), std::ios_base::app);
    csvFile << "SeqDrift;Selectivity & BranchMisses;";

    for (auto l = 0; l < 10; l++) {
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("f1", BasicType::INT64);
        schema->addField("f2", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        auto rd = std::random_device {};
        auto rng = std::default_random_engine {rd()};
        // generate list of values 1 til 100
        uint64_t fieldValuesSize = 100;
        std::vector<int64_t> fieldValues(fieldValuesSize);
        std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

        // generate tuple buffers
        uint64_t numberOfInputBuffers = 1000;
        std::vector<TupleBuffer> bufferVector;
        for (uint64_t i = 0; i < numberOfInputBuffers; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < numberOfTuplesInBuffer/100; j++) {
                std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
                for (uint64_t k = 0; k < fieldValuesSize; k++) {
                    dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[k]);
                    dynamicBuffer[(j * fieldValuesSize) + k]["f2"].write((int64_t) 1);
                    dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
                }
            }
        }

        // create pipeline
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(filterConstant);
        auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
        auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
        scanOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext();
        auto pipelineId = pipelineContext.getPipelineID();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage =
            std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        // initialize change detectors
        auto seqDrift = std::make_unique<SeqDrift2>(0.1, 200);
        auto seqDriftBranchMisses = std::make_unique<SeqDrift2>(0.1, 200);

        // initialize profiler
        auto profiler = std::make_shared<Profiler>();
        nautilusExecutablePipelineStage->setProfiler(profiler);

        // initialize statistics selectivity and branch misses
        auto pipelineSelectivity =
            std::make_unique<PipelineSelectivity>(std::move(seqDrift), nautilusExecutablePipelineStage);
        auto branchMisses = std::make_unique<BranchMisses>(std::move(seqDriftBranchMisses), profiler, 4);

        auto statisticsCollector = std::make_shared<StatisticsCollector>();
        statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));
        statisticsCollector->addStatistic(pipelineId, std::move(branchMisses));

        nautilusExecutablePipelineStage->setup(pipelineContext);
        auto runtimeStart = std::chrono::high_resolution_clock::now();
        for (auto& buffer : bufferVector) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
            nautilusExecutablePipelineStage->stop(statisticsCollector);
        }
        auto runtimeEnd = std::chrono::high_resolution_clock::now();
        nautilusExecutablePipelineStage->stop(pipelineContext);

        //auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);
        std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
        csvFile << duration.count() << ";";

        ASSERT_EQ(pipelineContext.buffers.size(), numberOfInputBuffers);
    }
    csvFile << "\n";
    csvFile.close();
}

/**
* @brief measure performance without statistics collection
*/
TEST_P(PerformanceTest, pipelinePerformanceTest) {

    csvFile.open(fmt::format("Performance_{}_{}.csv",numberOfTuplesInBuffer,filterConstant), std::ios_base::app);
    csvFile << "None;No Statistic;";

    for (auto l = 0; l < 10; l++) {
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("f1", BasicType::INT64);
        schema->addField("f2", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        auto rd = std::random_device {};
        auto rng = std::default_random_engine {rd()};
        // generate list of values 1 til 100
        uint64_t fieldValuesSize = 100;
        std::vector<int64_t> fieldValues(fieldValuesSize);
        std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

        // generate tuple buffers
        uint64_t numberOfInputBuffers = 1000;
        std::vector<TupleBuffer> bufferVector;
        for (uint64_t i = 0; i < numberOfInputBuffers; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < numberOfTuplesInBuffer/100; j++) {
                std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
                for (uint64_t k = 0; k < fieldValuesSize; k++) {
                    dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[k]);
                    dynamicBuffer[(j * fieldValuesSize) + k]["f2"].write((int64_t) 1);
                    dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
                }
            }
        }

        // create pipeline
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(filterConstant);
        auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
        auto selectionOperator = std::make_shared<Operators::Selection>(greaterThanExpression);
        scanOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage =
            std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        nautilusExecutablePipelineStage->setup(pipelineContext);
        auto runtimeStart = std::chrono::high_resolution_clock::now();
        for (auto& buffer : bufferVector) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        }
        auto runtimeEnd = std::chrono::high_resolution_clock::now();
        nautilusExecutablePipelineStage->stop(pipelineContext);

        //auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);
        std::chrono::duration<double, std::milli> duration = runtimeEnd - runtimeStart;
        csvFile << duration.count() << ";";

        ASSERT_EQ(pipelineContext.buffers.size(), numberOfInputBuffers);
    }
    csvFile << "\n";
    csvFile.close();
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        PerformanceTest,
                        ::testing::Values("PipelineCompiler"),
                        [](const testing::TestParamInfo<PerformanceTest::ParamType>& info) {
                            return info.param;
                        });

}