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

#include <TestUtils/UtilityFunctions.hpp>
#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Experimental/Vectorization/Kernel.hpp>
#include <Execution/Operators/Experimental/Vectorization/Unvectorize.hpp>
#include <Execution/Operators/Experimental/Vectorization/Vectorize.hpp>
#include <Execution/Operators/Experimental/Vectorization/VectorizedBatchAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {

class VectorizedBatchAggregationPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {
  public:
    Nautilus::CompilationOptions options;
    ExecutablePipelineProvider* provider{};
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("VectorizedBatchAggregationPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup VectorizedBatchAggregationPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup VectorizedBatchAggregationPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(NES::Runtime::Execution::VectorizedBatchAggregationPipelineTest::GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down VectorizedBatchAggregationPipelineTest test class."); }
};

/**
 * @brief Emit operator that emits a row oriented tuple buffer.
 */
TEST_P(VectorizedBatchAggregationPipelineTest, aggregationPipeline) {
    int numOfTuples = 64;
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::UINT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    uint64_t opHandlerIndex = 0;

    //scan
    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    //batchAggregation
    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();
    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {
        std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, readF1, "f1", "f1")};
    auto aggregationOp = std::make_shared<Operators::BatchAggregation>(opHandlerIndex, aggregationFunctions);
    //VectorizedBatchAggregation
    auto aggMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto vAggregationOp = std::make_shared<Operators::VectorizedBatchAggregation>(std::move(aggregationOp), std::move(aggMemoryProviderPtr));

    //vectorization
    auto vMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto uvMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto vectorizeOp =  std::make_shared<Operators::Vectorize>(opHandlerIndex, std::move(vMemoryProviderPtr));
    std::vector<Record::RecordFieldIdentifier> projections = {"f1"};
    auto unvectorizeOp = std::make_shared<Operators::Unvectorize>(std::move(uvMemoryProviderPtr), projections);

    //emit
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));

    //pipeline
    struct Operators::Kernel::Descriptor desc;
    desc.pipeline = vAggregationOp;
    desc.compileOptions = Util::createVectorizedCompilerOptions();
    desc.inputSchemaSize = schema->getSize();
    desc.threadsPerBlock = 32;
    auto kernel = std::make_shared<Operators::Kernel>(desc);
    scanOperator->setChild(vectorizeOp);
    vectorizeOp->setChild(kernel);
    kernel->setChild(unvectorizeOp);
    unvectorizeOp->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);

    // Fill buffer
    for(uint64_t i = 0; i < static_cast<uint64_t>(numOfTuples); i++) {
        dynamicBuffer[i]["f1"].write(i);
    }
    dynamicBuffer.setNumberOfTuples(numOfTuples);
    buffer.setWatermark(20);
    buffer.setSequenceNumber(1);
    buffer.setOriginId(0);

    auto aggExecutablePipeline = provider->create(pipeline, options);
    auto aggregationHandler = std::make_shared<Operators::BatchAggregationHandler>();
    auto pipelineContext = MockedPipelineExecutionContext({aggregationHandler});

    aggExecutablePipeline->setup(pipelineContext);
    aggExecutablePipeline->execute(buffer, pipelineContext, *wc);
    aggExecutablePipeline->stop(pipelineContext);

    auto emitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    emitSchema = emitSchema->addField("f1", BasicType::UINT64);
    auto emitMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(emitSchema, bm->getBufferSize());
    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(emitMemoryLayout, pipelineContext.buffers[0]);
    EXPECT_EQ(resultDynamicBuffer[0]["f1"].read<int64_t>(), 2080);
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        VectorizedBatchAggregationPipelineTest,
                        ::testing::Values("PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<VectorizedBatchAggregationPipelineTest::ParamType>& info) {
                            return info.param;
                        });

} // namespace NES::Runtime::Execution