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
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Map.hpp>
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
#include <Execution/StatisticsCollector/PipelineRuntime.hpp>
#include <Execution/StatisticsCollector/PipelineSelectivity.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <random>

namespace NES::Runtime::Execution {

class SelectivityTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SelectivityTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup SelectivityTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup SelectivityTest test case." << std::endl;
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>((8*1024), 1048576);
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down SelectivityTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down SelectivityTest test class." << std::endl; }
};

/**
* @brief collect runtime for selectivities from 0.01 to 1.0 on multiple buffers with randomly shuffled values [1,100]
*/
TEST_P(SelectivityTest, runtimeTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    schema->addField("f3", BasicType::INT64);
    schema->addField("f4", BasicType::INT64);
    schema->addField("f5", BasicType::INT64);
    schema->addField("f6", BasicType::INT64);
    schema->addField("f7", BasicType::INT64);
    schema->addField("f8", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto rd = std::random_device {};
    auto rng = std::default_random_engine {rd()};
    // generate list of values 1 til 100
    uint64_t fieldValuesSize = 100;
    std::vector<int64_t> fieldValues(fieldValuesSize);
    std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

    // generate tuple buffers
    uint64_t numberOfInputBuffers = 510000;
    std::vector<TupleBuffer> bufferVector;
    for (uint64_t i = 0; i < numberOfInputBuffers; ++i){
        auto buffer = bm->getBufferBlocking();
        bufferVector.push_back(buffer);
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t j = 0; j < 1; j++) {
            std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
            for (uint64_t k = 0; k < fieldValuesSize; k++) {
                dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[k]);
                dynamicBuffer[(j * fieldValuesSize) + k]["f2"].write((int64_t) 1);
                dynamicBuffer[(j * fieldValuesSize) + k]["f3"].write((int64_t) 2);
                dynamicBuffer[(j * fieldValuesSize) + k]["f4"].write((int64_t) 3);
                dynamicBuffer[(j * fieldValuesSize) + k]["f5"].write((int64_t) 4);
                dynamicBuffer[(j * fieldValuesSize) + k]["f6"].write((int64_t) 5);
                dynamicBuffer[(j * fieldValuesSize) + k]["f7"].write((int64_t) 6);
                dynamicBuffer[(j * fieldValuesSize) + k]["f8"].write((int64_t) 7);
                dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
            }
        }
    }

    std::ofstream csvFile("RuntimeTest.csv");

    for (int j = 1; j <= 100; ++j) {

        // create pipeline
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        // change filter condition with every iteration to get higher selectivity
        auto constantExpression = std::make_shared<Expressions::ConstantIntegerExpression>(j+1);
        auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto lessThanExpression = std::make_shared<Expressions::LessThanExpression>(readF1, constantExpression);
        auto selectionOperator = std::make_shared<Operators::Selection>(lessThanExpression);
        scanOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        auto adwinRuntime = std::make_unique<Adwin>(0.001, 4);
        auto changeDetectorWrapperRuntime = std::make_unique<ChangeDetectorWrapper>(std::move(adwinRuntime));

        // initialize statistic pipeline runtime
        auto runtime = std::make_unique<PipelineRuntime>(std::move(changeDetectorWrapperRuntime), nautilusExecutablePipelineStage, 1000);

        csvFile << "Selectivity " << ((double) j / 100);

        nautilusExecutablePipelineStage->setup(pipelineContext);
        for (auto buffer : bufferVector) {
            executablePipelineStage->execute(buffer, pipelineContext, *wc);

            runtime->collect();
            csvFile << ";" << std::any_cast<uint64_t>(runtime->getStatisticValue());
        }
        executablePipelineStage->stop(pipelineContext);

        csvFile << "\n";

        auto numberOfResultBuffers = (uint64_t) pipelineContext.buffers.size();
        ASSERT_EQ(numberOfResultBuffers, numberOfInputBuffers);
    }

    csvFile.close();
}

/**
* @brief collect branch misses for selectivities from 0.01 to 1.0 on multiple buffers with randomly shuffled values [1,100]
*/
TEST_P(SelectivityTest, branchMissesTest) {
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
    for (uint64_t i = 0; i < numberOfInputBuffers; ++i){
        auto buffer = bm->getBufferBlocking();
        bufferVector.push_back(buffer);
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t j = 0; j < 5; j++) {
            std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
            for (uint64_t k = 0; k < fieldValuesSize; k++) {
                dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[k]);
                dynamicBuffer[(j * fieldValuesSize) + k]["f2"].write((int64_t) 1);
                dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
            }
        }
    }

    std::ofstream csvFile("BranchMissesTest.csv");

    for (int j = 1; j <= 100; ++j) {

        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto constantExpression = std::make_shared<Expressions::ConstantIntegerExpression>(j+1);
        auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto lessThanExpression = std::make_shared<Expressions::LessThanExpression>(readF1, constantExpression);
        auto selectionOperator = std::make_shared<Operators::Selection>(lessThanExpression);
        scanOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto executablePipeline = provider->create(pipeline);
        auto pipelineContext = MockedPipelineExecutionContext();
        std::shared_ptr<ExecutablePipelineStage> executablePipelineStage = std::move(executablePipeline);
        auto nautilusExecutablePipelineStage = std::dynamic_pointer_cast<NautilusExecutablePipelineStage>(executablePipelineStage);

        auto adwinBranchMisses = std::make_unique<Adwin>(0.001, 4);
        auto changeDetectorWrapperBranch = std::make_unique<ChangeDetectorWrapper>(std::move(adwinBranchMisses));

        auto profiler = std::make_shared<Profiler>();
        nautilusExecutablePipelineStage->setProfiler(profiler);

        // initialize statistic branch misses
        auto branchMisses = std::make_unique<BranchMisses>(std::move(changeDetectorWrapperBranch), profiler, 10);

        //auto adwin = std::make_unique<Adwin>(0.001, 4);
        //auto changeDetectorWrapper = std::make_unique<ChangeDetectorWrapper>(std::move(adwin));
        //auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapper), nautilusExecutablePipelineStage);

        double sel = 0.0;
        csvFile << "Selectivity\t" << ((double) j / 100);

        nautilusExecutablePipelineStage->setup(pipelineContext);
        for (auto buffer : bufferVector) {
            executablePipelineStage->execute(buffer, pipelineContext, *wc);

            //pipelineSelectivity->collect();
            //sel = std::any_cast<double>(pipelineSelectivity->getStatisticValue());
            branchMisses->collect();
            csvFile << ";" << std::any_cast<uint64_t>(branchMisses->getStatisticValue());
        }
        executablePipelineStage->stop(pipelineContext);

        csvFile << "\n";

        //std::cout << "Selectivity " << sel << std::endl;

        auto numberOfResultBuffers = (uint64_t) pipelineContext.buffers.size();
        ASSERT_EQ(numberOfResultBuffers, numberOfInputBuffers);
    }

    csvFile.close();
}

/**
* @brief collect cache misses for selectivities from 0.05 to 1.0 on multiple buffers with randomly shuffled values [1,100]
*/
TEST_P(SelectivityTest, cacheMissesTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    schema->addField("f3", BasicType::INT64);
    schema->addField("f4", BasicType::INT64);
    schema->addField("f5", BasicType::INT64);
    schema->addField("f6", BasicType::INT64);
    schema->addField("f7", BasicType::INT64);
    schema->addField("f8", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto rd = std::random_device {};
    auto rng = std::default_random_engine {rd()};
    // generate list of values 1 til 100
    uint64_t fieldValuesSize = 100;
    std::vector<int64_t> fieldValues(fieldValuesSize);
    std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

    // generate tuple buffers
    uint64_t numberOfInputBuffers = 260000;
    std::vector<TupleBuffer> bufferVector;
    for (uint64_t i = 0; i < numberOfInputBuffers; ++i){
        auto buffer = bm->getBufferBlocking();
        bufferVector.push_back(buffer);
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t j = 0; j < 1; j++) {
            std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
            for (uint64_t k = 0; k < fieldValuesSize; k++) {
                dynamicBuffer[(j * fieldValuesSize) + k]["f1"].write(fieldValues[k]);
                dynamicBuffer[(j * fieldValuesSize) + k]["f2"].write((int64_t) 1);
                dynamicBuffer[(j * fieldValuesSize) + k]["f3"].write((int64_t) 2);
                dynamicBuffer[(j * fieldValuesSize) + k]["f4"].write((int64_t) 3);
                dynamicBuffer[(j * fieldValuesSize) + k]["f5"].write((int64_t) 4);
                dynamicBuffer[(j * fieldValuesSize) + k]["f6"].write((int64_t) 5);
                dynamicBuffer[(j * fieldValuesSize) + k]["f7"].write((int64_t) 6);
                dynamicBuffer[(j * fieldValuesSize) + k]["f8"].write((int64_t) 7);
                dynamicBuffer.setNumberOfTuples((j * fieldValuesSize) + k + 1);
            }
        }
    }

    std::ofstream csvFile("CacheMissesTest.csv");

    for (int j = 1; j <= 100; j++){

        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto constantExpression = std::make_shared<Expressions::ConstantIntegerExpression>(j+1);
        auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
        auto lessThanExpression = std::make_shared<Expressions::LessThanExpression>(readF1, constantExpression);
        auto selectionOperator = std::make_shared<Operators::Selection>(lessThanExpression);
        scanOperator->setChild(selectionOperator);

        // add map operator
        auto constantInt2 = std::make_shared<Expressions::ConstantIntegerExpression>(2);
        auto addExpression = std::make_shared<Expressions::MulExpression>(readF1, constantInt2);
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

        // initialize statistic cache misses
        auto adwinCacheMisses = std::make_unique<Adwin>(0.001, 4);
        auto changeDetectorWrapperBranch = std::make_unique<ChangeDetectorWrapper>(std::move(adwinCacheMisses));
        auto profiler = std::make_shared<Profiler>();
        nautilusExecutablePipelineStage->setProfiler(profiler);
        auto cacheMisses = std::make_unique<CacheMisses>(std::move(changeDetectorWrapperBranch), profiler, 100);

        auto adwin = std::make_unique<Adwin>(0.001, 4);
        auto changeDetectorWrapper = std::make_unique<ChangeDetectorWrapper>(std::move(adwin));
        auto pipelineSelectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapper), nautilusExecutablePipelineStage);

        double sel;
        auto sum = 0;

        nautilusExecutablePipelineStage->setup(pipelineContext);
        for (auto buffer : bufferVector) {
            executablePipelineStage->execute(buffer, pipelineContext, *wc);

            pipelineSelectivity->collect();
            sel = std::any_cast<double>(pipelineSelectivity->getStatisticValue());
            cacheMisses->collect();
            sum += std::any_cast<uint64_t>(cacheMisses->getStatisticValue());
            //csvFile << std::any_cast<uint64_t>(cacheMisses->getStatisticValue()) << "\n";
        }
        executablePipelineStage->stop(pipelineContext);

        // collect mean only
        //double mean = (double) sum / (double) pipelineContext.buffers.size();
        csvFile << sel << ";" << sum << "\n";

        auto numberOfResultBuffers = (uint64_t) pipelineContext.buffers.size();
        ASSERT_EQ(numberOfResultBuffers, numberOfInputBuffers);
    }

    csvFile.close();
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        SelectivityTest,
                        ::testing::Values("PipelineCompiler"),
                        [](const testing::TestParamInfo<SelectivityTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Runtime::Execution