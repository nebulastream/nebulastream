//
// Created by Juliane on 13.01.2023.
//
#include "Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp"
#include "Execution/Expressions/LogicalExpressions/LessThanExpression.hpp"
#include <API/Schema.hpp>
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
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

class SelectivityRuntimeTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SelectivityRuntimeTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup SelectivityRuntimeTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup SelectivityRuntimeTest test case." << std::endl;
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down SelectivityRuntimeTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down SelectivityRuntimeTest test class." << std::endl; }
};

/**
 * @brief 2 pipelines with different selectivity
 */
TEST_P(SelectivityRuntimeTest, selectivityTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // create first pipeline with selectivity of 10 %
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(5);
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto equalsExpression = std::make_shared<Expressions::EqualsExpression>(constantInt, readF1);
    auto selectionOperator = std::make_shared<Operators::Selection>(equalsExpression);
    scanOperator->setChild(selectionOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    selectionOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    // create second pipeline with selectivity of 80 %
    auto scanMemoryProviderPtr2 = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator2 = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr2));

    auto constantInt2 = std::make_shared<Expressions::ConstantIntegerExpression>(1);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1,constantInt2);
    auto selectionOperator2 = std::make_shared<Operators::Selection>(greaterThanExpression);
    scanOperator2->setChild(selectionOperator2);

    auto emitMemoryProviderPtr2=std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator2=std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr2));
    selectionOperator2->setChild(emitOperator2);

    auto pipeline2=std::make_shared<PhysicalOperatorPipeline>();
    pipeline2->setRootOperator(scanOperator2);

    // generate values in random order
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> dist(0, 10);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 100; i++) {
        uint64_t randomNumber = dist(rng);
        dynamicBuffer[i]["f1"].write((int64_t) randomNumber);
        dynamicBuffer[i]["f2"].write((int64_t) 1);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = provider->create(pipeline);

    auto pipelineContext = MockedPipelineExecutionContext();
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), pipelineContext.getNumberOfEmittedTuples());

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < resultBuffer.getNumberOfTuples(); i++) {
        ASSERT_EQ(resultDynamicBuffer[i]["f1"].read<int64_t>(), 5);
        ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int64_t>(), 1);
    }

    // apply pipeline2
    auto executablePipeline2 = provider->create(pipeline2);

    auto pipelineContext2 = MockedPipelineExecutionContext();
    executablePipeline2->setup(pipelineContext2);
    executablePipeline2->execute(buffer,pipelineContext2,*wc);
    executablePipeline2->stop(pipelineContext2);

    ASSERT_EQ(pipelineContext2.buffers.size(),1);
    auto resultBuffer2 = pipelineContext2.buffers[0];
    ASSERT_EQ(resultBuffer2.getNumberOfTuples(), pipelineContext2.getNumberOfEmittedTuples());

    auto resultDynamicBuffer2 = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout,resultBuffer2);
    for(uint64_t i = 0; i < resultBuffer2.getNumberOfTuples(); i++){
        ASSERT_GT(resultDynamicBuffer2[i]["f1"].read<int64_t>(),resultDynamicBuffer2[i]["f2"].read<int64_t>());
    }
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        SelectivityRuntimeTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<SelectivityRuntimeTest::ParamType>& info) {
                            return info.param;
                        });

/**
 * @brief 2 pipelines with different runtimes
 */
TEST_P(SelectivityRuntimeTest, runtimesTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // create first pipeline with selectivity of 10 %
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto constantInt = std::make_shared<Expressions::ConstantIntegerExpression>(5);
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto equalsExpression = std::make_shared<Expressions::EqualsExpression>(constantInt, readF1);
    auto selectionOperator = std::make_shared<Operators::Selection>(equalsExpression);
    scanOperator->setChild(selectionOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    selectionOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    // create second pipeline selectivity of 50 %
    auto scanMemoryProviderPtr2 = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator2 = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr2));

    auto readField1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto constInt1 = std::make_shared<Expressions::ConstantIntegerExpression>(2);
    auto greaterThanExpression = std::make_shared<Expressions::GreaterThanExpression>(readField1,constInt1);
    auto selectionOperator2_1 = std::make_shared<Operators::Selection>(greaterThanExpression);
    scanOperator2->setChild(selectionOperator2_1);

    auto constInt2 = std::make_shared<Expressions::ConstantIntegerExpression>(8);
    auto lessThanExpression = std::make_shared<Expressions::LessThanExpression>(readField1,constInt2);
    auto selectionOperator2_2 = std::make_shared<Operators::Selection>(lessThanExpression);
    selectionOperator2_1->setChild(selectionOperator2_2);

    auto emitMemoryProviderPtr2=std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator2=std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr2));
    selectionOperator2_2->setChild(emitOperator2);

    auto pipeline2=std::make_shared<PhysicalOperatorPipeline>();
    pipeline2->setRootOperator(scanOperator2);

    // generate values in random order
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> dist(0, 10);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 500; i++) {
        uint64_t randomNumber = dist(rng);
        dynamicBuffer[i]["f1"].write((int64_t) randomNumber);
        dynamicBuffer[i]["f2"].write((int64_t) 1);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = provider->create(pipeline);

    auto pipelineContext = MockedPipelineExecutionContext();
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), pipelineContext.getNumberOfEmittedTuples());

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 10; i++) {
        ASSERT_EQ(resultDynamicBuffer[i]["f1"].read<int64_t>(), 5);
        ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int64_t>(), 1);
    }

    // apply pipeline2
    auto executablePipeline2 = provider->create(pipeline2);

    auto pipelineContext2 = MockedPipelineExecutionContext();
    executablePipeline2->setup(pipelineContext2);
    executablePipeline2->execute(buffer,pipelineContext2,*wc);
    executablePipeline2->stop(pipelineContext2);

    ASSERT_EQ(pipelineContext2.buffers.size(),1);
    auto resultBuffer2 = pipelineContext2.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), pipelineContext.getNumberOfEmittedTuples());

    auto resultDynamicBuffer2 = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout,resultBuffer2);
    for(uint64_t i = 0; i < 10; i++){
        ASSERT_GT(resultDynamicBuffer[i]["f1"].read<int64_t>(), 2);
        ASSERT_LT(resultDynamicBuffer[i]["f1"].read<int64_t>(), 8);
        ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int64_t>(), 1);
    }
}

/**
 * @brief 2 buffers with different runtimes
 */
TEST_P(SelectivityRuntimeTest, runtimeBufferTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ConstantIntegerExpression>(5);
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto equalsExpression = std::make_shared<Expressions::EqualsExpression>(readF1, readF2);
    auto selectionOperator = std::make_shared<Operators::Selection>(equalsExpression);
    scanOperator->setChild(selectionOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    selectionOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    // generate values in random order
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> dist(0, 10);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 500; i++) {
        uint64_t randomNumber = dist(rng);
        dynamicBuffer[i]["f1"].write((int64_t) randomNumber);
        dynamicBuffer[i]["f2"].write((int64_t) 1);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    auto buffer2 = bm->getBufferBlocking();
    auto dynamicBuffer2 = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer2);
    for (uint64_t i = 0; i < 500; i++) {
        dynamicBuffer2[i]["f1"].write((int64_t) 5);
        dynamicBuffer2[i]["f2"].write((int64_t) 1);
        dynamicBuffer2.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = provider->create(pipeline);

    auto pipelineContext = MockedPipelineExecutionContext();
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), pipelineContext.getNumberOfEmittedTuples());

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 10; i++) {
        ASSERT_EQ(resultDynamicBuffer[i]["f1"].read<int64_t>(), 5);
        ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int64_t>(), 1);
    }

    // apply pipeline on buffer2
    auto executablePipeline2 = provider->create(pipeline);

    auto pipelineContext2 = MockedPipelineExecutionContext();
    executablePipeline2->setup(pipelineContext2);
    executablePipeline2->execute(buffer2,pipelineContext2,*wc);
    executablePipeline2->stop(pipelineContext2);

    ASSERT_EQ(pipelineContext2.buffers.size(),1);
    auto resultBuffer2 = pipelineContext2.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), pipelineContext.getNumberOfEmittedTuples());

    auto resultDynamicBuffer2 = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout,resultBuffer2);
    for(uint64_t i = 0; i < 100; i++){
        ASSERT_EQ(resultDynamicBuffer2[i]["f1"].read<int64_t>(), 5);
        ASSERT_EQ(resultDynamicBuffer2[i]["f2"].read<int64_t>(), 1);
    }
}

/**
 * @brief test multiple selectivities with multiple buffers
 */
TEST_P(SelectivityRuntimeTest, selectivityBuffersTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    // generate values in random order
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> dist(0, 10);

    std::vector<TupleBuffer> bufferVector;

    for (int j = 0; j < 3; ++j){
        auto buffer = bm->getBufferBlocking();
        bufferVector.push_back(buffer);
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t i = 0; i < 500; i++) {
            uint64_t randomNumber = dist(rng);
            dynamicBuffer[i]["f1"].write((int64_t) randomNumber);
            dynamicBuffer[i]["f2"].write((int64_t) 1);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }
    }

    for (int i = 1; i < 21; ++i) {

        std::cout << "Test Pipeline with selectivity of " << ((double)i/20) << "%." << std::endl;

        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto constantExpression = std::make_shared<Expressions::ConstantIntegerExpression>(i);
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
        executablePipeline->setup(pipelineContext);
        for (TupleBuffer buffer : bufferVector) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }
        executablePipeline->stop(pipelineContext);

        auto numberOfResultBuffers = (uint64_t) pipelineContext.buffers.size();
        ASSERT_EQ(numberOfResultBuffers, 3);
        // Todo: vector of number of emitted tuples?
        ASSERT_EQ(pipelineContext.buffers[2].getNumberOfTuples(), pipelineContext.getNumberOfEmittedTuples());

        auto resultBuffer = pipelineContext.buffers[0];
        auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
        for (uint64_t k = 0; k < resultBuffer.getNumberOfTuples(); ++k) {
            ASSERT_LT(resultDynamicBuffer[k]["f1"].read<int64_t>(), i);
            ASSERT_EQ(resultDynamicBuffer[k]["f2"].read<int64_t>(), 1);
        }
    }
}

}// namespace NES::Runtime::Execution
