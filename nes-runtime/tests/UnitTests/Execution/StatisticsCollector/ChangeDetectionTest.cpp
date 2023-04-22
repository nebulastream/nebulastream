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
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/OrExpression.hpp>
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
#include <Execution/StatisticsCollector/ChangeDataGenerator.hpp>
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

class ChangeDetectionTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ChangeDetectionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ChangeDetectionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup ChangeDetectionTest test case." << std::endl;
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>((8*1024), 32768);
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down ChangeDetectionTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ChangeDetectionTest test class." << std::endl; }
};

/**
 * @brief test change detection with Adwin
 */
TEST_P(ChangeDetectionTest, changeDetectionAdwinTest) {
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
    distVector.push_back(std::uniform_int_distribution<int64_t>(0, 3));
    distVector.push_back(std::uniform_int_distribution<int64_t>(0, 10));
    distVector.push_back(std::uniform_int_distribution<int64_t>(8, 10));

    for(auto& dist : distVector) {
        for (int i = 0; i < 500; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < 500; j++) {
                int64_t randomNumber = dist(rng);
                dynamicBuffer[j]["f1"].write(randomNumber);
                dynamicBuffer[j]["f2"].write((int64_t) 1);
                dynamicBuffer.setNumberOfTuples(j + 1);
            }
        }
    }

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    auto adwin = std::make_unique<Adwin>(0.001, 50);
    auto changeDetectorWrapper = std::make_unique<ChangeDetectorWrapper>(std::move(adwin));

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto pipelineId = pipelineContext.getPipelineID();

    auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapper), nautilusExecutablePipelineStage);

    auto statisticsCollector = std::make_shared<StatisticsCollector>();
    statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        nautilusExecutablePipelineStage->stop(statisticsCollector);
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1500);
}

/**
 * @brief test change detection with SeqDrift2
 */
TEST_P(ChangeDetectionTest, changeDetectionSeqDriftTest) {
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
    distVector.push_back(std::uniform_int_distribution<int64_t>(0, 3));
    distVector.push_back(std::uniform_int_distribution<int64_t>(0, 10));
    distVector.push_back(std::uniform_int_distribution<int64_t>(8, 10));

    for(auto& dist : distVector) {
        for (int i = 0; i < 500; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < 500; j++) {
                int64_t randomNumber = dist(rng);
                dynamicBuffer[j]["f1"].write(randomNumber);
                dynamicBuffer[j]["f2"].write((int64_t) 1);
                dynamicBuffer.setNumberOfTuples(j + 1);
            }
        }
    }

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    auto seqDrift = std::make_unique<SeqDrift2>(0.01, 50);
    auto changeDetectorWrapper = std::make_unique<ChangeDetectorWrapper>(std::move(seqDrift));

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto pipelineId = pipelineContext.getPipelineID();

    auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapper), nautilusExecutablePipelineStage);

    auto statisticsCollector = std::make_shared<StatisticsCollector>();
    statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        nautilusExecutablePipelineStage->stop(statisticsCollector);
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1500);
}

/**
 * @brief test change in the statistic runtime
 */
TEST_P(ChangeDetectionTest, runtimeChangeTest) {
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

    for(auto& dist : distVector) {
        for (int i = 0; i < 500; ++i) {
            auto buffer = bm->getBufferBlocking();
            bufferVector.push_back(buffer);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            for (uint64_t j = 0; j < 500; j++) {
                int64_t randomNumber = dist(rng);
                dynamicBuffer[j]["f1"].write(randomNumber);
                dynamicBuffer[j]["f2"].write((int64_t) 1);
                dynamicBuffer.setNumberOfTuples(j + 1);
            }
        }
    }

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    auto adwinRuntime = std::make_unique<Adwin>(0.001, 50);
    auto changeDetectorWrapper = std::make_unique<ChangeDetectorWrapper>(std::move(adwinRuntime));

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    // initialize statistic pipeline runtime
    auto pipelineRuntime = std::make_unique<PipelineRuntime>(std::move(changeDetectorWrapper), nautilusExecutablePipelineStage, 5);

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        for (uint64_t i = 0; i < 100; i++) {
            nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);

            // collect the statistics
            pipelineRuntime->collect();
        }
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 300);
}

/**
 * @brief test change in the statistic branch misses
 */
TEST_P(ChangeDetectionTest, branchMissesAbruptChangeTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // create pipeline
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(99);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
    auto selectionOperatorX = std::make_shared<Operators::Selection>(greaterThanExpression);
    scanOperator->setChild(selectionOperatorX);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    selectionOperatorX->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    // create buffers with distribution change
    auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::ABRUPT, 2500000);
    std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(10000);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto adwinSelectivity = std::make_unique<Adwin>(0.001, 50);
    auto changeDetectorWrapperSelectivity = std::make_unique<ChangeDetectorWrapper>(std::move(adwinSelectivity));

    auto adwinBranchMisses = std::make_unique<Adwin>(0.1, 3);
    auto changeDetectorWrapperBranch = std::make_unique<ChangeDetectorWrapper>(std::move(adwinBranchMisses));

    auto profiler = std::make_shared<Profiler>();
    nautilusExecutablePipelineStage->setProfiler(profiler);

    // initialize statistics pipeline statistics
    auto selectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapperSelectivity), nautilusExecutablePipelineStage);
    auto branchMisses = std::make_unique<BranchMisses>(std::move(changeDetectorWrapperBranch), profiler, 100);

    std::ofstream csvFile("BranchMissesAbruptChangeTest.csv");
    csvFile << "Selectivity, Branch misses, Change\n" ;

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);

        // collect the statistics
        selectivity->collect();
        auto change = branchMisses->collect();
        csvFile << std::any_cast<double>(selectivity->getStatisticValue()) << ", " << std::any_cast<uint64_t>(branchMisses->getStatisticValue()) <<  ", " << change <<"\n";
        if(change){
            std::cout << "Change detected" << std::endl;
        }
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 10000);
}

/**
 * @brief test change in the statistic branch misses
 */
TEST_P(ChangeDetectionTest, branchMissesIncrementalChangeTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // create pipeline
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(99);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
    auto selectionOperatorX = std::make_shared<Operators::Selection>(greaterThanExpression);
    scanOperator->setChild(selectionOperatorX);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    selectionOperatorX->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    // create buffers with distribution change
    auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::INCREMENTAL, 500000);
    std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(11000);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto adwinSelectivity = std::make_unique<Adwin>(0.001, 50);
    auto changeDetectorWrapperSelectivity = std::make_unique<ChangeDetectorWrapper>(std::move(adwinSelectivity));

    auto adwinBranchMisses = std::make_unique<Adwin>(0.1, 3);
    auto changeDetectorWrapperBranch = std::make_unique<ChangeDetectorWrapper>(std::move(adwinBranchMisses));

    auto profiler = std::make_shared<Profiler>();
    nautilusExecutablePipelineStage->setProfiler(profiler);

    // initialize statistics pipeline statistics
    auto selectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapperSelectivity), nautilusExecutablePipelineStage);
    auto branchMisses = std::make_unique<BranchMisses>(std::move(changeDetectorWrapperBranch), profiler, 100);

    std::ofstream csvFile("BranchMissesIncrementalChangeTest.csv");
    csvFile << "Selectivity, Branch misses, Change\n" ;

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);

        // collect the statistics
        selectivity->collect();
        auto change = branchMisses->collect();
        csvFile << std::any_cast<double>(selectivity->getStatisticValue()) << ", " << std::any_cast<uint64_t>(branchMisses->getStatisticValue()) <<  ", " << change <<"\n";
        if(change){
            std::cout << "Change detected" << std::endl;
        }
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 11000);
}


/**
 * @brief test change in the statistic branch misses
 */
TEST_P(ChangeDetectionTest, branchMissesGradualChangeTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // create pipeline
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(99);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
    auto selectionOperatorX = std::make_shared<Operators::Selection>(greaterThanExpression);
    scanOperator->setChild(selectionOperatorX);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    selectionOperatorX->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    // create buffers with distribution change
    auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::GRADUAL, 2000000);
    std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(11000);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto adwinSelectivity = std::make_unique<Adwin>(0.001, 4);
    auto changeDetectorWrapperSelectivity = std::make_unique<ChangeDetectorWrapper>(std::move(adwinSelectivity));

    auto adwinBranchMisses = std::make_unique<Adwin>(0.1, 3);
    auto changeDetectorWrapperBranch = std::make_unique<ChangeDetectorWrapper>(std::move(adwinBranchMisses));

    auto profiler = std::make_shared<Profiler>();
    nautilusExecutablePipelineStage->setProfiler(profiler);

    // initialize statistics pipeline statistics
    auto selectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapperSelectivity), nautilusExecutablePipelineStage);
    auto branchMisses = std::make_unique<BranchMisses>(std::move(changeDetectorWrapperBranch), profiler, 100);

    std::ofstream csvFile("BranchMissesGradualChangeTest.csv");
    csvFile << "Selectivity, Branch misses, Change\n" ;

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);

        // collect the statistics
        selectivity->collect();
        auto change =branchMisses->collect();
        csvFile << std::any_cast<double>(selectivity->getStatisticValue()) << ", " << std::any_cast<uint64_t>(branchMisses->getStatisticValue()) <<  ", " << change <<"\n";
        if(change){
            std::cout << "Change detected" << std::endl;
        }
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 11000);
}

/**
 * @brief test change in the statistic branch misses
 */
TEST_P(ChangeDetectionTest, reorderPointDetectionTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // create pipeline
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(99);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1, constantInt);
    auto selectionOperatorX = std::make_shared<Operators::Selection>(greaterThanExpression);
    scanOperator->setChild(selectionOperatorX);

    auto constantInt2 = std::make_shared<Expressions::ConstantIntegerExpression>(100);
    auto lessThanExpression = std::make_shared<Expressions::LessThanExpression>(readField1, constantInt2);
    auto equalsExpression = std::make_shared<Expressions::EqualsExpression>(readField1, constantInt2);
    auto orExpression = std::make_shared<Expressions::OrExpression>(lessThanExpression,equalsExpression);
    auto selectionOperatorY = std::make_shared<Operators::Selection>(orExpression);
    selectionOperatorX->setChild(selectionOperatorY);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    selectionOperatorY->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    // create buffers with distribution change
    auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::INCREMENTAL, 500000);
    std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(11000);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto adwinSelectivity = std::make_unique<Adwin>(0.001, 4);
    auto changeDetectorWrapperSelectivity = std::make_unique<ChangeDetectorWrapper>(std::move(adwinSelectivity));

    auto adwinBranchMisses = std::make_unique<Adwin>(0.001, 12);
    auto changeDetectorWrapperBranch = std::make_unique<ChangeDetectorWrapper>(std::move(adwinBranchMisses));

    auto profiler = std::make_shared<Profiler>();
    nautilusExecutablePipelineStage->setProfiler(profiler);

    // initialize statistics pipeline statistics
    auto selectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapperSelectivity), nautilusExecutablePipelineStage);
    auto branchMisses = std::make_unique<BranchMisses>(std::move(changeDetectorWrapperBranch), profiler, 100);

    std::ofstream csvFile("ReorderPointDetectionTest.csv");
    csvFile << "Selectivity,Branch Misses,Change\n" ;

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);

        // collect the statistics
        selectivity->collect();
        auto change = branchMisses->collect();
        csvFile << std::any_cast<double>(selectivity->getStatisticValue()) << ", " << std::any_cast<uint64_t>(branchMisses->getStatisticValue()) <<  ", " << change <<"\n";
        if(change){
            std::cout << "Change detected" << std::endl;
        }
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 11000);
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        ChangeDetectionTest,
                        ::testing::Values("PipelineCompiler"),
                        [](const testing::TestParamInfo<ChangeDetectionTest::ParamType>& info) {
                            return info.param;
                        });

}
