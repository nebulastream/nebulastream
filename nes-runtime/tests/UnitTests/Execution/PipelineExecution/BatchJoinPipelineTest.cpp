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
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Relational/Join/AntiBatchJoinProbe.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinBuild.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinHandler.hpp>
#include <Execution/Operators/Relational/Join/InnerBatchJoinProbe.hpp>
#include <Execution/Operators/Relational/Join/OuterBatchJoinProbe.hpp>
#include <Execution/Operators/Relational/Join/SemiBatchJoinProbe.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {

class BatchJoinPipelineTest : public Testing::BaseUnitTest, public AbstractPipelineExecutionTest {
  public:
    Nautilus::CompilationOptions options;
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("BatchJoinPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup BatchJoinPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup BatchJoinPipelineTest test case.");
        if (!ExecutablePipelineProviderRegistry::hasPlugin(GetParam())) {
            GTEST_SKIP();
        }
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down BatchJoinPipelineTest test class."); }
};

TEST_P(BatchJoinPipelineTest, joinBuildPipeline) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema = schema->addField("k1", BasicType::INT64)->addField("v1", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    resultSchema->addField("f1", BasicType::INT64);
    auto resultMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("k1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("v1");
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();

    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    std::vector<Expressions::ExpressionPtr> keyFields = {readF1};
    std::vector<Expressions::ExpressionPtr> valueFields = {readF2};
    std::vector<PhysicalTypePtr> types = {integerType};
    auto joinOp = std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                              keyFields,
                                                              types,
                                                              valueFields,
                                                              types,
                                                              std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

    scanOperator->setChild(joinOp);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0]["k1"].write(+1_s64);
    dynamicBuffer[0]["v1"].write(+10_s64);
    dynamicBuffer[1]["k1"].write(+1_s64);
    dynamicBuffer[1]["v1"].write(+1_s64);
    dynamicBuffer[2]["k1"].write(+2_s64);
    dynamicBuffer[2]["v1"].write(+2_s64);
    dynamicBuffer[3]["k1"].write(+3_s64);
    dynamicBuffer[3]["v1"].write(+10_s64);
    dynamicBuffer.setNumberOfTuples(4);
    buffer.setWatermark(20);
    buffer.setSequenceNumber(1);
    buffer.setOriginId(0);

    auto joinBuildExecutablePipeline = provider->create(pipeline, options);
    auto joinHandler = std::make_shared<Operators::BatchJoinHandler>();
    auto pipeline1Context = MockedPipelineExecutionContext({joinHandler});
    joinBuildExecutablePipeline->setup(pipeline1Context);
    joinBuildExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    joinBuildExecutablePipeline->stop(pipeline1Context);
    auto entries = joinHandler->getThreadLocalState(wc->getId())->getNumberOfEntries();
    ASSERT_EQ(entries, 4_u64);
}

TEST_P(BatchJoinPipelineTest, innerJoinPipeline) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema = schema->addField("k1", BasicType::INT64)->addField("v1", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("k1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("v1");
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();

    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    std::vector<Expressions::ExpressionPtr> keyFields = {readF1};
    std::vector<Expressions::ExpressionPtr> valueFields = {readF2};
    std::vector<PhysicalTypePtr> types = {integerType};
    auto joinOp = std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                              keyFields,
                                                              types,
                                                              valueFields,
                                                              types,
                                                              std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

    scanOperator->setChild(joinOp);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);

    // Fill buffer
    dynamicBuffer[0]["k1"].write(+1_s64);
    dynamicBuffer[0]["v1"].write(+10_s64);
    dynamicBuffer[1]["k1"].write(+1_s64);
    dynamicBuffer[1]["v1"].write(+1_s64);
    dynamicBuffer[2]["k1"].write(+2_s64);
    dynamicBuffer[2]["v1"].write(+2_s64);
    dynamicBuffer[3]["k1"].write(+3_s64);
    dynamicBuffer[3]["v1"].write(+10_s64);
    dynamicBuffer.setNumberOfTuples(4);
    buffer.setWatermark(20);
    buffer.setSequenceNumber(1);
    buffer.setOriginId(0);

    auto joinBuildExecutablePipeline = provider->create(pipeline, options);
    auto joinHandler = std::make_shared<Operators::BatchJoinHandler>();
    auto pipeline1Context = MockedPipelineExecutionContext({joinHandler});
    joinBuildExecutablePipeline->setup(pipeline1Context);
    joinBuildExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    joinBuildExecutablePipeline->stop(pipeline1Context);

    // Perform inner join
    auto pipeline2 = std::make_shared<PhysicalOperatorPipeline>();
    auto joinProbe =
        std::make_shared<Operators::InnerBatchJoinProbe>(0 /*handler index*/,
                                                         keyFields,
                                                         types,
                                                         std::vector<Record::RecordFieldIdentifier>(),
                                                         std::vector<PhysicalTypePtr>(),
                                                         std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
    pipeline2->setRootOperator(joinProbe);

    auto joinProbeExecutablePipeline = provider->create(pipeline2, options);
    auto pipeline2Context = MockedPipelineExecutionContext({joinHandler});
    joinProbeExecutablePipeline->setup(pipeline2Context);
    joinProbeExecutablePipeline->execute(buffer, pipeline2Context, *wc);
    joinProbeExecutablePipeline->stop(pipeline2Context);

    auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    resultSchema->addField("revenue", BasicType::FLOAT32);
    auto resultLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(resultLayout, pipeline2.ctx->buffers[0]);
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        BatchJoinPipelineTest,
                        ::testing::Values("PipelineInterpreter", "BCInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        [](const testing::TestParamInfo<BatchJoinPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution