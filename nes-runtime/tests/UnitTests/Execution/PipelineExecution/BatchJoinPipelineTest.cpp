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
#include <Execution/Operators/Emit.hpp>
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

// Performs an inner join on tuples with distinct keys
// Thus each tuple in R will join with a tuple in S and vice versa
TEST_P(BatchJoinPipelineTest, innerJoinPipelineWithDistinctKeys) {
    constexpr size_t NUMBER_TUPLES = 100;
    auto schema =
        Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("k1", BasicType::INT64)->addField("v1", BasicType::INT64);
    auto memoryLayout1 = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout1);
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
    auto dynamicBuffer1 = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout1, buffer);

    // Fill buffer
    for (size_t i = 0; i < NUMBER_TUPLES; ++i) {
        dynamicBuffer1[i]["k1"].write((int64_t) i);
        dynamicBuffer1[i]["v1"].write((int64_t) (10 * i));
    }
    dynamicBuffer1.setNumberOfTuples(NUMBER_TUPLES);
    buffer.setWatermark(20);
    buffer.setSequenceNumber(1);
    buffer.setOriginId(0);

    auto joinBuildExecutablePipeline = provider->create(pipeline, options);
    auto joinHandler = std::make_shared<Operators::BatchJoinHandler>();
    auto pipeline1Context = MockedPipelineExecutionContext({joinHandler});
    joinBuildExecutablePipeline->setup(pipeline1Context);
    joinBuildExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    joinBuildExecutablePipeline->stop(pipeline1Context);

    // Merge local join state into global state to prepare to probe
    joinHandler->mergeState();

    // Perform inner join
    auto pipeline2 = std::make_shared<PhysicalOperatorPipeline>();

    std::vector<Expressions::ExpressionPtr> probeKeys = {std::make_shared<Expressions::ReadFieldExpression>("k2")};

    auto schema2 =
        Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("k2", BasicType::INT64)->addField("v2", BasicType::INT64);
    auto memoryProvider = std::make_unique<MemoryProvider::RowMemoryProvider>(
        Runtime::MemoryLayouts::RowLayout::create(schema2, bm->getBufferSize()));
    auto scanOperator2 = std::make_shared<Operators::Scan>(std::move(memoryProvider));
    pipeline2->setRootOperator(scanOperator2);

    auto joinProbe =
        std::make_shared<Operators::InnerBatchJoinProbe>(0 /*handler index*/,
                                                         probeKeys,
                                                         std::vector<PhysicalTypePtr>{integerType},
                                                         std::vector<Record::RecordFieldIdentifier>{"v1"},
                                                         std::vector<PhysicalTypePtr>{integerType},
                                                         std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

    scanOperator2->setChild(joinProbe);

    auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                            ->addField("k2", BasicType::INT64)
                            ->addField("v1", BasicType::INT64)
                            ->addField("v2", BasicType::INT64);
    auto resultMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(resultMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    joinProbe->setChild(emitOperator);

    auto buffer2 = bm->getBufferBlocking();
    auto memoryLayout2 = Runtime::MemoryLayouts::RowLayout::create(schema2, bm->getBufferSize());
    auto dynamicBuffer2 = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout2, buffer2);

    // Fill buffer
    for (size_t i = 0; i < NUMBER_TUPLES; ++i) {
        dynamicBuffer2[i]["k2"].write((int64_t) i);
        dynamicBuffer2[i]["v2"].write((int64_t) (11 * i));
    }
    dynamicBuffer2.setNumberOfTuples(NUMBER_TUPLES);
    buffer2.setWatermark(20);
    buffer2.setSequenceNumber(1);
    buffer2.setOriginId(0);

    auto joinProbeExecutablePipeline = provider->create(pipeline2, options);
    auto pipeline2Context = MockedPipelineExecutionContext({joinHandler});
    joinProbeExecutablePipeline->setup(pipeline2Context);
    joinProbeExecutablePipeline->execute(buffer2, pipeline2Context, *wc);
    joinProbeExecutablePipeline->stop(pipeline2Context);

    ASSERT_EQ(pipeline2Context.buffers.size(), 1);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(resultMemoryLayout, pipeline2Context.buffers[0]);
    ASSERT_EQ(resultDynamicBuffer.getNumberOfTuples(), NUMBER_TUPLES);
    for (size_t i = 0; i < NUMBER_TUPLES; ++i) {
        EXPECT_EQ(resultDynamicBuffer[i]["k2"].read<int64_t>(), i);
        EXPECT_EQ(resultDynamicBuffer[i]["v1"].read<int64_t>(), 10 * i);
        EXPECT_EQ(resultDynamicBuffer[i]["v2"].read<int64_t>(), 11 * i);
    }
}

// Performs an inner with duplicate keys
// Thus, the tuples of R will join with multiple tuples of S and vice versa
TEST_P(BatchJoinPipelineTest, innerJoinPipelineWithDuplicateKeys) {
    constexpr size_t NUMBER_TUPLES = 4;
    constexpr size_t MODULO = 2;
    constexpr size_t EXPECTED_TUPLE_NUM = 8;

    auto schema =
        Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("k1", BasicType::INT64)->addField("v1", BasicType::INT64);
    auto memoryLayout1 = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout1);
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
    auto dynamicBuffer1 = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout1, buffer);

    // Fill buffer
    for (size_t i = 0; i < NUMBER_TUPLES; ++i) {
        dynamicBuffer1[i]["k1"].write((int64_t) (i % MODULO));
        dynamicBuffer1[i]["v1"].write((int64_t) i);
    }
    dynamicBuffer1.setNumberOfTuples(NUMBER_TUPLES);
    buffer.setWatermark(20);
    buffer.setSequenceNumber(1);
    buffer.setOriginId(0);

    auto joinBuildExecutablePipeline = provider->create(pipeline, options);
    auto joinHandler = std::make_shared<Operators::BatchJoinHandler>();
    auto pipeline1Context = MockedPipelineExecutionContext({joinHandler});
    joinBuildExecutablePipeline->setup(pipeline1Context);
    joinBuildExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    joinBuildExecutablePipeline->stop(pipeline1Context);

    // Merge local join state into global state to prepare to probe
    joinHandler->mergeState();

    // Perform inner join
    auto pipeline2 = std::make_shared<PhysicalOperatorPipeline>();

    std::vector<Expressions::ExpressionPtr> probeKeys = {std::make_shared<Expressions::ReadFieldExpression>("k2")};

    auto schema2 =
        Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("k2", BasicType::INT64)->addField("v2", BasicType::INT64);
    auto memoryProvider = std::make_unique<MemoryProvider::RowMemoryProvider>(
        Runtime::MemoryLayouts::RowLayout::create(schema2, bm->getBufferSize()));
    auto scanOperator2 = std::make_shared<Operators::Scan>(std::move(memoryProvider));
    pipeline2->setRootOperator(scanOperator2);

    auto joinProbe =
        std::make_shared<Operators::InnerBatchJoinProbe>(0 /*handler index*/,
                                                         probeKeys,
                                                         std::vector<PhysicalTypePtr>{integerType},
                                                         std::vector<Record::RecordFieldIdentifier>{"v1"},
                                                         std::vector<PhysicalTypePtr>{integerType},
                                                         std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

    scanOperator2->setChild(joinProbe);

    auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                            ->addField("k2", BasicType::INT64)
                            ->addField("v1", BasicType::INT64)
                            ->addField("v2", BasicType::INT64);
    auto resultMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(resultMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    joinProbe->setChild(emitOperator);

    auto buffer2 = bm->getBufferBlocking();
    auto memoryLayout2 = Runtime::MemoryLayouts::RowLayout::create(schema2, bm->getBufferSize());
    auto dynamicBuffer2 = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout2, buffer2);

    // Fill buffer
    for (size_t i = 0; i < NUMBER_TUPLES; ++i) {
        dynamicBuffer2[i]["k2"].write((int64_t) (i % MODULO));
        dynamicBuffer2[i]["v2"].write((int64_t) i);
    }
    dynamicBuffer2.setNumberOfTuples(NUMBER_TUPLES);
    buffer2.setWatermark(20);
    buffer2.setSequenceNumber(1);
    buffer2.setOriginId(0);

    auto joinProbeExecutablePipeline = provider->create(pipeline2, options);
    auto pipeline2Context = MockedPipelineExecutionContext({joinHandler});
    joinProbeExecutablePipeline->setup(pipeline2Context);
    joinProbeExecutablePipeline->execute(buffer2, pipeline2Context, *wc);
    joinProbeExecutablePipeline->stop(pipeline2Context);

    ASSERT_EQ(pipeline2Context.buffers.size(), 1);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(resultMemoryLayout, pipeline2Context.buffers[0]);
    ASSERT_EQ(resultDynamicBuffer.getNumberOfTuples(), EXPECTED_TUPLE_NUM);
    // All combinations of tuples with same key should be joined
    EXPECT_EQ(resultDynamicBuffer[0]["k2"].read<int64_t>(), 0);
    EXPECT_EQ(resultDynamicBuffer[0]["v1"].read<int64_t>(), 2);
    EXPECT_EQ(resultDynamicBuffer[0]["v2"].read<int64_t>(), 0);

    EXPECT_EQ(resultDynamicBuffer[1]["k2"].read<int64_t>(), 0);
    EXPECT_EQ(resultDynamicBuffer[1]["v1"].read<int64_t>(), 0);
    EXPECT_EQ(resultDynamicBuffer[1]["v2"].read<int64_t>(), 0);

    EXPECT_EQ(resultDynamicBuffer[2]["k2"].read<int64_t>(), 1);
    EXPECT_EQ(resultDynamicBuffer[2]["v1"].read<int64_t>(), 3);
    EXPECT_EQ(resultDynamicBuffer[2]["v2"].read<int64_t>(), 1);

    EXPECT_EQ(resultDynamicBuffer[3]["k2"].read<int64_t>(), 1);
    EXPECT_EQ(resultDynamicBuffer[3]["v1"].read<int64_t>(), 1);
    EXPECT_EQ(resultDynamicBuffer[3]["v2"].read<int64_t>(), 1);

    EXPECT_EQ(resultDynamicBuffer[4]["k2"].read<int64_t>(), 0);
    EXPECT_EQ(resultDynamicBuffer[4]["v1"].read<int64_t>(), 2);
    EXPECT_EQ(resultDynamicBuffer[4]["v2"].read<int64_t>(), 2);

    EXPECT_EQ(resultDynamicBuffer[5]["k2"].read<int64_t>(), 0);
    EXPECT_EQ(resultDynamicBuffer[5]["v1"].read<int64_t>(), 0);
    EXPECT_EQ(resultDynamicBuffer[5]["v2"].read<int64_t>(), 2);

    EXPECT_EQ(resultDynamicBuffer[6]["k2"].read<int64_t>(), 1);
    EXPECT_EQ(resultDynamicBuffer[6]["v1"].read<int64_t>(), 3);
    EXPECT_EQ(resultDynamicBuffer[6]["v2"].read<int64_t>(), 3);

    EXPECT_EQ(resultDynamicBuffer[7]["k2"].read<int64_t>(), 1);
    EXPECT_EQ(resultDynamicBuffer[7]["v1"].read<int64_t>(), 1);
    EXPECT_EQ(resultDynamicBuffer[7]["v2"].read<int64_t>(), 3);
}

// Performs a semi join
// To test if duplicates are eliminated we add duplicates to the probe side
TEST_P(BatchJoinPipelineTest, SemiJoinPipeline) {
    constexpr size_t NUMBER_TUPLES = 100;
    auto schema =
        Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("k1", BasicType::INT64)->addField("v1", BasicType::INT64);
    auto memoryLayout1 = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout1);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("k1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("v1");
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();

    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    PhysicalTypePtr byteType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt8());

    std::vector<Expressions::ExpressionPtr> keyFields = {readF1};
    std::vector<Expressions::ExpressionPtr> valueFields = {readF2};
    std::vector<PhysicalTypePtr> keyTypes = {integerType, byteType};// Add Byte Type for Semi Join making logic
    std::vector<PhysicalTypePtr> valueTypes = {integerType};

    auto joinOp = std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                              keyFields,
                                                              keyTypes,
                                                              valueFields,
                                                              valueTypes,
                                                              std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

    scanOperator->setChild(joinOp);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer1 = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout1, buffer);

    // Fill buffer
    for (size_t i = 0; i < NUMBER_TUPLES; ++i) {
        dynamicBuffer1[i]["k1"].write((int64_t) i);
        dynamicBuffer1[i]["v1"].write((int64_t) (10 * i));
    }
    dynamicBuffer1.setNumberOfTuples(NUMBER_TUPLES);
    buffer.setWatermark(20);
    buffer.setSequenceNumber(1);
    buffer.setOriginId(0);

    auto joinBuildExecutablePipeline = provider->create(pipeline, options);
    auto joinHandler = std::make_shared<Operators::BatchJoinHandler>();
    auto pipeline1Context = MockedPipelineExecutionContext({joinHandler});
    joinBuildExecutablePipeline->setup(pipeline1Context);
    joinBuildExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    joinBuildExecutablePipeline->stop(pipeline1Context);

    // Merge local join state into global state to prepare to probe
    joinHandler->mergeState();

    // Perform inner join
    auto pipeline2 = std::make_shared<PhysicalOperatorPipeline>();

    std::vector<Expressions::ExpressionPtr> probeKeys = {std::make_shared<Expressions::ReadFieldExpression>("k2")};

    auto schema2 =
        Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("k2", BasicType::INT64)->addField("v2", BasicType::INT64);
    auto memoryProvider = std::make_unique<MemoryProvider::RowMemoryProvider>(
        Runtime::MemoryLayouts::RowLayout::create(schema2, bm->getBufferSize()));
    auto scanOperator2 = std::make_shared<Operators::Scan>(std::move(memoryProvider));
    pipeline2->setRootOperator(scanOperator2);

    auto joinProbe =
        std::make_shared<Operators::SemiBatchJoinProbe>(0 /*handler index*/,
                                                        probeKeys,
                                                        std::vector<PhysicalTypePtr>{integerType, byteType},// for mark logic
                                                        std::vector<Record::RecordFieldIdentifier>{"v1"},
                                                        std::vector<PhysicalTypePtr>{integerType},
                                                        std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

    scanOperator2->setChild(joinProbe);

    auto resultSchema =
        Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("k2", BasicType::INT64)->addField("v2", BasicType::INT64);
    auto resultMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(resultMemoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    joinProbe->setChild(emitOperator);

    auto buffer2 = bm->getBufferBlocking();
    auto memoryLayout2 = Runtime::MemoryLayouts::RowLayout::create(schema2, bm->getBufferSize());
    auto dynamicBuffer2 = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout2, buffer2);

    // Fill buffer
    for (size_t i = 0; i < NUMBER_TUPLES; ++i) {
        if (i < 3) {
            // Add duplicates to check if they are not emitted
            dynamicBuffer2[i]["k2"].write((int64_t) 0);
            dynamicBuffer2[i]["v2"].write((int64_t) 0);
        } else {
            dynamicBuffer2[i]["k2"].write((int64_t) i);
            dynamicBuffer2[i]["v2"].write((int64_t) (11 * i));
        }
    }

    dynamicBuffer2.setNumberOfTuples(NUMBER_TUPLES);
    buffer2.setWatermark(20);
    buffer2.setSequenceNumber(1);
    buffer2.setOriginId(0);

    auto joinProbeExecutablePipeline = provider->create(pipeline2, options);
    auto pipeline2Context = MockedPipelineExecutionContext({joinHandler});
    joinProbeExecutablePipeline->setup(pipeline2Context);
    joinProbeExecutablePipeline->execute(buffer2, pipeline2Context, *wc);
    joinProbeExecutablePipeline->stop(pipeline2Context);

    ASSERT_EQ(pipeline2Context.buffers.size(), 1);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(resultMemoryLayout, pipeline2Context.buffers[0]);
    constexpr size_t WITHOUT_DUPLICATES = NUMBER_TUPLES - 2;
    ASSERT_EQ(resultDynamicBuffer.getNumberOfTuples(), WITHOUT_DUPLICATES);
    EXPECT_EQ(resultDynamicBuffer[0]["k2"].read<int64_t>(), 0);
    EXPECT_EQ(resultDynamicBuffer[0]["v2"].read<int64_t>(), 0);
    for (size_t i = 1; i < WITHOUT_DUPLICATES; ++i) {
        EXPECT_EQ(resultDynamicBuffer[i]["k2"].read<int64_t>(), i + 2);
        EXPECT_EQ(resultDynamicBuffer[i]["v2"].read<int64_t>(), 11 * (i + 2));
    }
}

TEST_P(BatchJoinPipelineTest, AntiJoinPipeline) {
    constexpr size_t NUMBER_TUPLES = 100;
    auto schema =
        Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("k1", BasicType::INT64)->addField("v1", BasicType::INT64);
    auto memoryLayout1 = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout1);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("k1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("v1");
    auto physicalTypeFactory = DefaultPhysicalTypeFactory();

    PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt64());
    PhysicalTypePtr byteType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt8());

    std::vector<Expressions::ExpressionPtr> keyFields = {readF1};
    std::vector<Expressions::ExpressionPtr> valueFields = {readF2};
    std::vector<PhysicalTypePtr> keyTypes = {integerType, byteType};
    std::vector<PhysicalTypePtr> valueTypes = {integerType};
    auto joinOp = std::make_shared<Operators::BatchJoinBuild>(0 /*handler index*/,
                                                              keyFields,
                                                              keyTypes,
                                                              valueFields,
                                                              valueTypes,
                                                              std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

    scanOperator->setChild(joinOp);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer1 = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout1, buffer);

    // Fill buffer
    for (size_t i = 0; i < NUMBER_TUPLES; ++i) {
        dynamicBuffer1[i]["k1"].write((int64_t) i);
        dynamicBuffer1[i]["v1"].write((int64_t) (10 * i));
    }
    dynamicBuffer1.setNumberOfTuples(NUMBER_TUPLES);
    buffer.setWatermark(20);
    buffer.setSequenceNumber(1);
    buffer.setOriginId(0);

    auto joinBuildExecutablePipeline = provider->create(pipeline, options);
    auto joinHandler = std::make_shared<Operators::BatchJoinHandler>();
    auto pipeline1Context = MockedPipelineExecutionContext({joinHandler});
    joinBuildExecutablePipeline->setup(pipeline1Context);
    joinBuildExecutablePipeline->execute(buffer, pipeline1Context, *wc);
    joinBuildExecutablePipeline->stop(pipeline1Context);

    // Merge local join state into global state to prepare to probe
    joinHandler->mergeState();

    // Perform inner join
    auto pipeline2 = std::make_shared<PhysicalOperatorPipeline>();

    std::vector<Expressions::ExpressionPtr> probeKeys = {std::make_shared<Expressions::ReadFieldExpression>("k2")};

    auto schema2 =
        Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("k2", BasicType::INT64)->addField("v2", BasicType::INT64);
    auto memoryProvider = std::make_unique<MemoryProvider::RowMemoryProvider>(
        Runtime::MemoryLayouts::RowLayout::create(schema2, bm->getBufferSize()));
    auto scanOperator2 = std::make_shared<Operators::Scan>(std::move(memoryProvider));
    pipeline2->setRootOperator(scanOperator2);

    auto resultSchema =
        Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField("k2", BasicType::INT64)->addField("v2", BasicType::INT64);
    auto resultMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(resultSchema, bm->getBufferSize());
    auto memoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(resultMemoryLayout);

    auto joinProbe = std::make_shared<Operators::AntiBatchJoinProbe>(
        0 /*handler index*/,
        probeKeys,
        std::vector<PhysicalTypePtr>{integerType, byteType},   // for mark logic
        std::vector<Record::RecordFieldIdentifier>{"k1", "v1"},// this changed here maybe add a field for the keys
        std::vector<PhysicalTypePtr>{integerType},
        std::make_unique<Nautilus::Interface::MurMur3HashFunction>(),
        std::move(memoryProviderPtr));

    scanOperator2->setChild(joinProbe);

    //auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    //joinProbe->setChild(emitOperator);

    auto buffer2 = bm->getBufferBlocking();
    auto memoryLayout2 = Runtime::MemoryLayouts::RowLayout::create(schema2, bm->getBufferSize());
    auto dynamicBuffer2 = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout2, buffer2);

    // Fill buffer
    for (size_t i = 0; i < NUMBER_TUPLES; ++i) {
        dynamicBuffer2[i]["k2"].write((int64_t) i);
        dynamicBuffer2[i]["v2"].write((int64_t) (11 * i));
    }
    dynamicBuffer2.setNumberOfTuples(NUMBER_TUPLES);
    buffer2.setWatermark(20);
    buffer2.setSequenceNumber(1);
    buffer2.setOriginId(0);

    auto joinProbeExecutablePipeline = provider->create(pipeline2, options);
    auto pipeline2Context = MockedPipelineExecutionContext({joinHandler});
    joinProbeExecutablePipeline->setup(pipeline2Context);
    joinProbeExecutablePipeline->execute(buffer2, pipeline2Context, *wc);
    joinProbeExecutablePipeline->stop(pipeline2Context);

    ASSERT_EQ(pipeline2Context.buffers.size(), 1);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(resultMemoryLayout, pipeline2Context.buffers[0]);
    ASSERT_EQ(resultDynamicBuffer.getNumberOfTuples(), NUMBER_TUPLES);
    for (size_t i = 0; i < NUMBER_TUPLES; ++i) {
        EXPECT_EQ(resultDynamicBuffer[i]["k2"].read<int64_t>(), i);
        EXPECT_EQ(resultDynamicBuffer[i]["v2"].read<int64_t>(), 11 * i);
    }
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        BatchJoinPipelineTest,
                        // TODO issue X disabled for now as we are currently only supporting interpreted pipelines
                        //::testing::Values("PipelineInterpreter", "BCInterpreter", "PipelineCompiler", "CPPPipelineCompiler"),
                        ::testing::Values("PipelineInterpreter"),
                        [](const testing::TestParamInfo<BatchJoinPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution