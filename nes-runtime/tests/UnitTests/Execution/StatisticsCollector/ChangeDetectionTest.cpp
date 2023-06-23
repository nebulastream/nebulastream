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
#include <Execution/StatisticsCollector/ChangeDataGenerator.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/Adwin/Adwin.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/SeqDrift2/SeqDrift2.hpp>
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
        bm = std::make_shared<Runtime::BufferManager>((16*1024), 262144);
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
TEST_P(ChangeDetectionTest, selectivityAdwinTest) {
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

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);
    auto adwin = std::make_unique<Adwin>(0.8, 5);
    auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(adwin), nautilusExecutablePipelineStage);

    //auto pipelineId = pipelineContext.getPipelineID();
    //auto statisticsCollector = std::make_shared<StatisticsCollector>();
    //statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        //nautilusExecutablePipelineStage->stop(statisticsCollector);
        auto change = pipelineSelectivity->collect();
        if(change){
            std::cout << "Change detected" << std::endl;
        }
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1500);
}

/**
 * @brief test change detection with SeqDrift2
 */
TEST_P(ChangeDetectionTest, selectivitySeqDriftTest) {
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

    auto seqDrift = std::make_unique<SeqDrift2>(0.1, 5);
    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);
    auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(seqDrift), nautilusExecutablePipelineStage);

    //auto pipelineId = pipelineContext.getPipelineID();
    //auto statisticsCollector = std::make_shared<StatisticsCollector>();
    //statisticsCollector->addStatistic(pipelineId, std::move(pipelineSelectivity));

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        //nautilusExecutablePipelineStage->stop(statisticsCollector);
        auto change = pipelineSelectivity->collect();
        if(change){
            std::cout << "Change detected" << std::endl;
        }
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1500);
}

/**
 * @brief test change in the statistic runtime
 */
TEST_P(ChangeDetectionTest, runtimeAdwinTest) {
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
    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    // initialize statistic pipeline runtime
    auto pipelineRuntime = std::make_unique<PipelineRuntime>(std::move(adwinRuntime), nautilusExecutablePipelineStage, 5);

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
TEST_P(ChangeDetectionTest, branchMissesAbruptChangeAdwinDeltaTest) {
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

    // create buffers with abrupt distribution change
    auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::ABRUPT, 2000000);
    std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(6000);

    // create buffers with incremental distribution change
    //auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::INCREMENTAL, 2000000);
    //std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(22000);

    // create buffers with gradual distribution change
    //auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::GRADUAL, 2000000);
    //std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(6000);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto adwinSelectivity = std::make_unique<Adwin>(0.01, 5);
    auto adwinBranchMisses = std::make_unique<Adwin>(0.01, 5);
    auto profiler = std::make_shared<Profiler>();
    nautilusExecutablePipelineStage->setProfiler(profiler);

    // initialize statistics pipeline statistics
    auto selectivity = std::make_unique<PipelineSelectivity>(std::move(adwinSelectivity), nautilusExecutablePipelineStage);
    auto branchMisses = std::make_unique<BranchMisses>(std::move(adwinBranchMisses), profiler, 10);

    // collect selectivity and branch misses
    std::vector<double> selectivityValues;
    std::vector<uint64_t> branchMissesValues;

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);

        // collect the statistics
        selectivity->collect();
        selectivityValues.push_back(std::any_cast<double>(selectivity->getStatisticValue()));
        branchMisses->collect();
        branchMissesValues.push_back(std::any_cast<uint64_t>(branchMisses->getStatisticValue()));
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 6000);

    // try different deltas for Adwin
    std::vector<double> deltas = {0.001,0.005,0.01,0.05,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1};

    for (int i = 0; i < (int)deltas.size(); i ++) {
        std::ofstream csvFile(fmt::format("ChangeAbruptAdwinDeltaTest{}.csv",i));
        csvFile << "Selectivity;Branch misses;Change\n";

        auto adwinTest = std::make_unique<Adwin>(deltas[i], 5);
        auto normalizerTest = Normalizer(10, std::move(adwinTest));

        for (uint64_t j = 0; j < (uint64_t)branchMissesValues.size(); ++j) {
            auto change = normalizerTest.normalizeValue(branchMissesValues[j]);
            csvFile << selectivityValues[j] << ";" << branchMissesValues[j] <<";" << change << "\n";
        }

        csvFile.close();
    }
}

/**
 * @brief test change in the statistic branch misses
 */
TEST_P(ChangeDetectionTest, branchMissesAbruptChangeAdwinBucketsTest) {
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
    auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::ABRUPT, 2000000);
    std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(6000);

    // create buffers with incremental distribution change
    //auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::INCREMENTAL, 2000000);
    //std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(22000);

    // create buffers with gradual distribution change
    //auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::GRADUAL, 2000000);
    //std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(6000);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto adwinSelectivity = std::make_unique<Adwin>(0.01, 5);
    auto adwinBranchMisses = std::make_unique<Adwin>(0.01, 5);
    auto profiler = std::make_shared<Profiler>();
    nautilusExecutablePipelineStage->setProfiler(profiler);

    // initialize statistics pipeline statistics
    auto selectivity = std::make_unique<PipelineSelectivity>(std::move(adwinSelectivity), nautilusExecutablePipelineStage);
    auto branchMisses = std::make_unique<BranchMisses>(std::move(adwinBranchMisses), profiler, 10);

    // collect selectivity and branch misses
    std::vector<double> selectivityValues;
    std::vector<uint64_t> branchMissesValues;

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);

        // collect the statistics
        selectivity->collect();
        selectivityValues.push_back(std::any_cast<double>(selectivity->getStatisticValue()));
        branchMisses->collect();
        branchMissesValues.push_back(std::any_cast<uint64_t>(branchMisses->getStatisticValue()));
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 6000);

    // try different number of buckets for Adwin
    std::vector<int> buckets = {2,3,4,5,8,16,32,64,128};

    for (int i = 0; i < (int)buckets.size(); i ++) {
        std::ofstream csvFile(fmt::format("ChangeAbruptAdwinBucketsTest{}.csv",i));
        csvFile << "Selectivity;Branch misses;Change\n";

        auto adwinTest = std::make_unique<Adwin>(0.6, buckets[i]);
        auto normalizerTest = Normalizer(10, std::move(adwinTest));

        for (uint64_t j = 0; j < (uint64_t)branchMissesValues.size(); ++j) {
            auto change = normalizerTest.normalizeValue(branchMissesValues[j]);
            csvFile << selectivityValues[j] << ";" << branchMissesValues[j] <<";" << change << "\n";
        }

        csvFile.close();
    }
}

/**
 * @brief test change in the statistic branch misses
 */
TEST_P(ChangeDetectionTest, seqDriftAbruptChangeDeltaTest) {
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
    auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::ABRUPT, 2000000);
    std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(6000);

    // create buffers with incremental distribution change
    //auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::INCREMENTAL, 2000000);
    //std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(22000);

    // create buffers with gradual distribution change
    //auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::GRADUAL, 2000000);
    //std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(6000);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto adwinSelectivity = std::make_unique<Adwin>(0.01, 5);
    auto seqDriftBranchMisses = std::make_unique<SeqDrift2>(0.01, 200);
    auto profiler = std::make_shared<Profiler>();
    nautilusExecutablePipelineStage->setProfiler(profiler);

    // initialize statistics pipeline statistics
    auto selectivity = std::make_unique<PipelineSelectivity>(std::move(adwinSelectivity), nautilusExecutablePipelineStage);
    auto branchMisses = std::make_unique<BranchMisses>(std::move(seqDriftBranchMisses), profiler, 10);

    // collect selectivity and branch misses
    std::vector<double> selectivityValues;
    std::vector<uint64_t> branchMissesValues;

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);

        // collect the statistics
        selectivity->collect();
        selectivityValues.push_back(std::any_cast<double>(selectivity->getStatisticValue()));
        branchMisses->collect();
        branchMissesValues.push_back(std::any_cast<uint64_t>(branchMisses->getStatisticValue()));
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 6000);

    // try different deltas for SeqDrift
    std::vector<double> deltas = {0.001,0.005,0.01,0.05,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1};

    for (int i = 0; i < (int)deltas.size(); i ++) {
        std::ofstream csvFile(fmt::format("ChangeAbruptSeqDriftDeltaTest{}.csv",i));
        csvFile << "Selectivity;Branch misses;Change\n";

        auto seqDriftTest = std::make_unique<SeqDrift2>(deltas[i], 200);
        auto normalizerTest = Normalizer(10, std::move(seqDriftTest));

        for (uint64_t j = 0; j < (uint64_t)branchMissesValues.size(); ++j) {
            auto change = normalizerTest.normalizeValue(branchMissesValues[j]);
            csvFile << selectivityValues[j] << ";" << branchMissesValues[j] <<";" << change << "\n";
        }

        csvFile.close();
    }
}

/**
 * @brief test change in the statistic branch misses
 */
TEST_P(ChangeDetectionTest, seqDriftAbruptChangeMTest) {
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
    auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::ABRUPT, 2000000);
    std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(6000);

    // create buffers with incremental distribution change
    //auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::INCREMENTAL, 2000000);
    //std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(22000);

    // create buffers with gradual distribution change
    //auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::GRADUAL, 2000000);
    //std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(6000);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto adwinSelectivity = std::make_unique<Adwin>(0.01, 5);
    auto seqDriftBranchMisses = std::make_unique<SeqDrift2>(0.01, 200);
    auto profiler = std::make_shared<Profiler>();
    nautilusExecutablePipelineStage->setProfiler(profiler);

    // initialize statistics pipeline statistics
    auto selectivity = std::make_unique<PipelineSelectivity>(std::move(adwinSelectivity), nautilusExecutablePipelineStage);
    auto branchMisses = std::make_unique<BranchMisses>(std::move(seqDriftBranchMisses), profiler, 10);

    // collect selectivity and branch misses
    std::vector<double> selectivityValues;
    std::vector<uint64_t> branchMissesValues;

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);

        // collect the statistics
        selectivity->collect();
        selectivityValues.push_back(std::any_cast<double>(selectivity->getStatisticValue()));
        branchMisses->collect();
        branchMissesValues.push_back(std::any_cast<uint64_t>(branchMisses->getStatisticValue()));
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 6000);

    // try different deltas for SeqDrift
    std::vector<int> blocks = {50,100,200,250,300,400,500,600,700};

    for (int i = 0; i < (int)blocks.size(); i ++) {
        std::ofstream csvFile(fmt::format("ChangeAbruptSeqDriftBlocksTest{}.csv",i));
        csvFile << "Selectivity;Branch misses;Change\n";

        auto seqDriftTest = std::make_unique<SeqDrift2>(0.9, blocks[i]);
        auto normalizerTest = Normalizer(10, std::move(seqDriftTest));

        for (uint64_t j = 0; j < (uint64_t)branchMissesValues.size(); ++j) {
            auto change = normalizerTest.normalizeValue(branchMissesValues[j]);
            csvFile << selectivityValues[j] << ";" << branchMissesValues[j] <<";" << change << "\n";
        }

        csvFile.close();
    }
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
    auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::INCREMENTAL, 2000000);
    std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(22000);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto adwinSelectivity = std::make_unique<Adwin>(0.1, 5);
    auto adwinBranchMisses = std::make_unique<Adwin>(0.6, 5);
    auto profiler = std::make_shared<Profiler>();
    nautilusExecutablePipelineStage->setProfiler(profiler);

    // initialize statistics pipeline statistics
    auto selectivity = std::make_unique<PipelineSelectivity>(std::move(adwinSelectivity), nautilusExecutablePipelineStage);
    auto branchMisses = std::make_unique<BranchMisses>(std::move(adwinBranchMisses), profiler, 100);

    std::ofstream csvFile("BranchMissesIncrementalChangeTest.csv");
    csvFile << "Selectivity;Branch misses;Change\n" ;

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);

        // collect the statistics
        selectivity->collect();
        auto change = branchMisses->collect();
        csvFile << std::any_cast<double>(selectivity->getStatisticValue()) << ";" << std::any_cast<uint64_t>(branchMisses->getStatisticValue()) <<  ";" << change <<"\n";
        if(change){
            std::cout << "Change detected" << std::endl;
        }
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 22000);

    csvFile.close();
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
    std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(6000);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();

    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto adwinSelectivity = std::make_unique<Adwin>(0.1, 5);
    auto adwinBranchMisses = std::make_unique<Adwin>(0.6, 5);
    auto profiler = std::make_shared<Profiler>();
    nautilusExecutablePipelineStage->setProfiler(profiler);

    // initialize statistics pipeline statistics
    auto selectivity = std::make_unique<PipelineSelectivity>(std::move(adwinSelectivity), nautilusExecutablePipelineStage);
    auto branchMisses = std::make_unique<BranchMisses>(std::move(adwinBranchMisses), profiler, 100);

    std::ofstream csvFile("BranchMissesGradualChangeTest.csv");
    csvFile << "Selectivity;Branch misses;Change\n" ;

    nautilusExecutablePipelineStage->setup(pipelineContext);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);

        // collect the statistics
        selectivity->collect();
        auto change =branchMisses->collect();
        csvFile << std::any_cast<double>(selectivity->getStatisticValue()) << ";" << std::any_cast<uint64_t>(branchMisses->getStatisticValue()) <<  ";" << change <<"\n";
        if(change){
            std::cout << "Change detected" << std::endl;
        }
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 6000);

    csvFile.close();
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

    auto readField2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto constantInt2 = std::make_shared<Expressions::ConstantIntegerExpression>(99);
    auto greaterThanExpressionY = std::make_shared<Expressions::GreaterThanExpression>(readField2, constantInt2);
    auto selectionOperatorY = std::make_shared<Operators::Selection>(greaterThanExpressionY);
    selectionOperatorX->setChild(selectionOperatorY);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    selectionOperatorY->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    // create buffers with distribution change
    auto changeDataGenerator = ChangeDataGenerator(bm, memoryLayout, Execution::ChangeType::REOCCURRING, 400000);
    std::vector<TupleBuffer> bufferVector = changeDataGenerator.generateBuffers(40000);

    auto executablePipeline = provider->create(pipeline);
    auto pipelineContext = MockedPipelineExecutionContext();
    std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
    auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

    auto adwinSelectivity = std::make_unique<Adwin>(0.9,5);
    auto adwinBranchMisses = std::make_unique<Adwin>(0.9,5);
    auto profiler = std::make_shared<Profiler>();
    nautilusExecutablePipelineStage->setProfiler(profiler);

    // initialize statistics pipeline statistics
    auto selectivity = std::make_unique<PipelineSelectivity>(std::move(adwinSelectivity), nautilusExecutablePipelineStage);
    auto branchMisses = std::make_unique<BranchMisses>(std::move(adwinBranchMisses), profiler, 100);

    // create pipeline X
    auto scanMemoryProviderPtrX = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperatorX = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtrX));

    auto operatorX = std::make_shared<Operators::Selection>(greaterThanExpression);
    scanOperatorX->setChild(operatorX);

    auto emitMemoryProviderPtrX = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperatorX = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtrX));
    operatorX->setChild(emitOperatorX);

    auto pipelineX = std::make_shared<PhysicalOperatorPipeline>();
    pipelineX->setRootOperator(scanOperatorX);

    auto executablePipelineX = provider->create(pipelineX);
    auto pipelineContextX = MockedPipelineExecutionContext();
    std::shared_ptr<ExecutablePipelineStage> executablePipelineStageX = std::move(executablePipelineX);
    auto nautilusExecutablePipelineStageX = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStageX);

    auto adwinSelectivityX = std::make_unique<Adwin>(0.9, 5);
    auto selectivityX = std::make_unique<PipelineSelectivity>(std::move(adwinSelectivityX), nautilusExecutablePipelineStageX);

    // create pipeline Y
    auto scanMemoryProviderPtrY = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperatorY = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtrY));

    auto operatorY = std::make_shared<Operators::Selection>(greaterThanExpressionY);
    scanOperatorY->setChild(operatorY);

    auto emitMemoryProviderPtrY = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperatorY = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtrY));
    operatorY->setChild(emitOperatorY);

    auto pipelineY = std::make_shared<PhysicalOperatorPipeline>();
    pipelineY->setRootOperator(scanOperatorY);

    auto executablePipelineY = provider->create(pipelineY);
    auto pipelineContextY = MockedPipelineExecutionContext();
    std::shared_ptr<ExecutablePipelineStage> executablePipelineStageY = std::move(executablePipelineY);
    auto nautilusExecutablePipelineStageY = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStageY);

    auto adwinSelectivityY = std::make_unique<Adwin>(0.9, 5);
    auto selectivityY = std::make_unique<PipelineSelectivity>(std::move(adwinSelectivityY), nautilusExecutablePipelineStageY);

    // collect selectivity and branch misses
    std::vector<double> selectivityValues;
    std::vector<double> selectivityXValues;
    std::vector<double> selectivityYValues;
    std::vector<uint64_t> branchMissesValues;

    nautilusExecutablePipelineStage->setup(pipelineContext);
    nautilusExecutablePipelineStageX->setup(pipelineContextX);
    nautilusExecutablePipelineStageY->setup(pipelineContextY);
    for (auto& buffer : bufferVector) {
        nautilusExecutablePipelineStage->execute(buffer, pipelineContext, *wc);
        nautilusExecutablePipelineStageX->execute(buffer, pipelineContextX, *wc);
        nautilusExecutablePipelineStageY->execute(buffer, pipelineContextY, *wc);

        // collect the statistics
        selectivity->collect();
        selectivityX->collect();
        selectivityY->collect();
        branchMisses->collect();
        selectivityValues.push_back(std::any_cast<double>(selectivity->getStatisticValue()));
        selectivityXValues.push_back(std::any_cast<double>(selectivityX->getStatisticValue()));
        selectivityYValues.push_back(std::any_cast<double>(selectivityY->getStatisticValue()));
        branchMissesValues.push_back(std::any_cast<uint64_t>(branchMisses->getStatisticValue()));
    }
    nautilusExecutablePipelineStage->stop(pipelineContext);
    nautilusExecutablePipelineStageX->stop(pipelineContextX);
    nautilusExecutablePipelineStageY->stop(pipelineContextY);


    // try different deltas for SeqDrift
    std::vector<double> deltas = {0.001,0.005,0.01,0.05,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1};

    for (int i = 0; i < (int)deltas.size(); i ++) {
        std::ofstream csvFile(fmt::format("ReorderAdwinDeltaTest{}.csv",i));
        csvFile << "Combined Selectivity;Selectivity X;Selectivity Y;Branch misses;Change\n";

        auto adwinTest = std::make_unique<Adwin>(deltas[i],5);
        auto normalizerTest = Normalizer(10, std::move(adwinTest));

        for (uint64_t j = 0; j < (uint64_t)branchMissesValues.size(); ++j) {
            auto change = normalizerTest.normalizeValue(branchMissesValues[j]);
            csvFile << selectivityValues[j] << ";" << selectivityXValues[j] << ";" << selectivityYValues[j] << ";" << branchMissesValues[j] <<";" << change <<"\n";
        }

        csvFile.close();
    }

    for (int i = 0; i < (int)deltas.size(); i ++) {
        std::ofstream csvFile2(fmt::format("ReorderSeqDriftDeltaTest{}.csv",i));
        csvFile2 << "Combined Selectivity;Selectivity X;Selectivity Y;Branch misses;Change\n";

        auto seqDriftTest = std::make_unique<SeqDrift2>(deltas[i],200);
        auto normalizerSeqDrift = Normalizer(10, std::move(seqDriftTest));

        for (uint64_t j = 0; j < (uint64_t)branchMissesValues.size(); ++j) {
            auto changeSeqDrift = normalizerSeqDrift.normalizeValue(branchMissesValues[j]);
            csvFile2 << selectivityValues[j] << ";" << selectivityXValues[j] << ";" << selectivityYValues[j] << ";" << branchMissesValues[j] <<";" << changeSeqDrift <<"\n";
        }

        csvFile2.close();
    }

}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        ChangeDetectionTest,
                        ::testing::Values("PipelineCompiler"),
                        [](const testing::TestParamInfo<ChangeDetectionTest::ParamType>& info) {
                            return info.param;
                        });

}
