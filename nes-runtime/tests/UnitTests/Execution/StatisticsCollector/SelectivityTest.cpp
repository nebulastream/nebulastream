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
#include <Execution/StatisticsCollector/ChangeDetectors/Adwin/Adwin.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/ChangeDetectorWrapper.hpp>
#include <Execution/StatisticsCollector/PipelineRuntime.hpp>
#include <Execution/StatisticsCollector/PipelineSelectivity.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
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
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down SelectivityTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down SelectivityTest test class." << std::endl; }
};

/**
* @brief collect runtime of multiple selectivities with multiple buffers
*/
TEST_P(SelectivityTest, runtimeTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // generate values in random order
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int64_t> dist(0, 20);

    std::vector<TupleBuffer> bufferVector;

    for (int i = 0; i < 1000; ++i){
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

    const char *fileRuntime = "RuntimeTest.txt";
    FILE* runtimeFile = fopen(fileRuntime, "a+");
    const char *fileSelectivity = "SelectivityTest.txt";
    FILE* selectivityFile = fopen(fileSelectivity, "a+");

    for (int j = 1; j < 21; ++j) {

        fprintf(selectivityFile, "Selectivity\t%f\n", ((double)j / 20)) ;
        fprintf(runtimeFile, "Selectivity\t%f\n", ((double)j / 20)) ;

        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto constantExpression = std::make_shared<Expressions::ConstantIntegerExpression>(j);
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

        auto adwinSelectivity = std::make_unique<Adwin>(0.001, 4);
        auto changeDetectorWrapperSelectivity = std::make_unique<ChangeDetectorWrapper>(std::move(adwinSelectivity));

        auto adwinRuntime = std::make_unique<Adwin>(0.001, 4);
        auto changeDetectorWrapperRuntime = std::make_unique<ChangeDetectorWrapper>(std::move(adwinRuntime));

        // initialize statistics pipeline runtime
        auto selectivity = std::make_unique<PipelineSelectivity>(std::move(changeDetectorWrapperSelectivity), nautilusExecutablePipelineStage);
        auto runtime = std::make_unique<PipelineRuntime>(std::move(changeDetectorWrapperRuntime), nautilusExecutablePipelineStage, 1000);

        nautilusExecutablePipelineStage->setup(pipelineContext);
        for (TupleBuffer buffer : bufferVector) {
            executablePipelineStage->execute(buffer, pipelineContext, *wc);

            selectivity->collect();
            runtime->collect();
            fprintf(selectivityFile, "%f\n", selectivity->getSelectivity());
            fprintf(runtimeFile, "%lu\n", runtime->getRuntime());
        }
        executablePipelineStage->stop(pipelineContext);

        auto numberOfResultBuffers = (uint64_t) pipelineContext.buffers.size();
        ASSERT_EQ(numberOfResultBuffers, 1000);
    }

    fclose(selectivityFile);
    fclose(runtimeFile);
}

/**
* @brief collect branch misses of multiple selectivities with multiple buffers
*/
TEST_P(SelectivityTest, branchMissesTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // generate values in random order
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int64_t> dist(0, 20);

    std::vector<TupleBuffer> bufferVector;

    for (int i = 0; i < 1000; ++i){
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

    const char *fileName = "BranchMissesTest.txt";
    FILE* outputFile = fopen(fileName, "a+");

    for (int j = 1; j < 21; ++j) {

        fprintf(outputFile, "Selectivity\t%f\n", ((double)j / 20)) ;

        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto constantExpression = std::make_shared<Expressions::ConstantIntegerExpression>(j);
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

        // initialize statistics pipeline runtime
        auto branchMisses = std::make_unique<BranchMisses>(std::move(changeDetectorWrapperBranch), profiler);

        nautilusExecutablePipelineStage->setup(pipelineContext);
        for (TupleBuffer buffer : bufferVector) {
            executablePipelineStage->execute(buffer, pipelineContext, *wc);

            branchMisses->collect();
            fprintf(outputFile, "%lu\n", branchMisses->getBranchMisses()) ;
        }
        executablePipelineStage->stop(pipelineContext);

        auto numberOfResultBuffers = (uint64_t) pipelineContext.buffers.size();
        ASSERT_EQ(numberOfResultBuffers, 1000);
    }

    fclose(outputFile);
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        SelectivityTest,
                        ::testing::Values("PipelineCompiler"),
                        [](const testing::TestParamInfo<SelectivityTest::ParamType>& info) {
                            return info.param;
                        });
}// namespace NES::Runtime::Execution