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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <DeltaOperatorHandler.hpp>
#include <DeltaPhysicalOperator.hpp>
#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <Pipeline.hpp>
#include <PipelineExecutionContext.hpp>
#include <ScanPhysicalOperator.hpp>
#include <TestTupleBuffer.hpp>

namespace NES
{

class DeltaPhysicalOperatorTest : public Testing::BaseUnitTest, public ::testing::WithParamInterface<bool>
{
    struct MockedPipelineContext final : PipelineExecutionContext
    {
        bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
        {
            emittedBuffers.push_back(buffer);
            return true;
        }

        TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

        [[nodiscard]] WorkerThreadId getId() const override { return INITIAL<WorkerThreadId>; }

        [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

        [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

        [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override
        {
            return *operatorHandlers;
        }

        void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& opHandlers) override
        {
            operatorHandlers = &opHandlers;
        }

        explicit MockedPipelineContext(std::shared_ptr<BufferManager> bufferManager) : bufferManager(std::move(bufferManager)) { }

        void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { INVARIANT(false, "This function should not be called"); }

        std::vector<TupleBuffer> emittedBuffers;
        std::shared_ptr<BufferManager> bufferManager;
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* operatorHandlers = nullptr;
    };

public:
    static void SetUpTestSuite() { Logger::setupLogging("DeltaPhysicalOperatorTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override { BaseUnitTest::SetUp(); }

    /// Builds the field layout metadata needed by the DeltaPhysicalOperator constructor.
    struct DeltaLayoutInfo
    {
        std::vector<DeltaFieldLayoutEntry> deltaFieldLayout;
        size_t deltaFieldsEntrySize;
        std::vector<DeltaFieldLayoutEntry> fullRecordLayout;
        size_t fullRecordEntrySize;
    };

    static DeltaLayoutInfo buildLayoutInfo(const std::vector<PhysicalDeltaExpression>& deltaExprs, const Schema& inputSchema)
    {
        DeltaLayoutInfo info{};
        for (const auto& expr : deltaExprs)
        {
            info.deltaFieldLayout.push_back({expr.targetField, expr.targetDataType.type});
            info.deltaFieldsEntrySize += expr.targetDataType.getSizeInBytes();
        }
        for (const auto& field : inputSchema.getFields())
        {
            info.fullRecordLayout.push_back({field.name, field.dataType.type});
            info.fullRecordEntrySize += field.dataType.getSizeInBytes();
        }
        return info;
    }

    /// Creates and executes a Scan -> Delta -> Emit pipeline over multiple input buffers.
    /// Returns the emitted output buffers.
    std::vector<TupleBuffer> runPipeline(
        const Schema& inputSchema,
        const Schema& outputSchema,
        std::vector<PhysicalDeltaExpression> deltaExprs,
        std::vector<TupleBuffer>& inputBuffers)
    {
        const auto isCompiled = GetParam();
        auto emitHandlerId = getNextOperatorHandlerId();
        auto deltaHandlerId = getNextOperatorHandlerId();

        auto layoutInfo = buildLayoutInfo(deltaExprs, inputSchema);
        auto deltaHandler = std::make_shared<DeltaOperatorHandler>(layoutInfo.deltaFieldsEntrySize, layoutInfo.fullRecordEntrySize);

        auto scanBufRef = LowerSchemaProvider::lowerSchema(BufferSize, inputSchema, MemoryLayoutType::ROW_LAYOUT);
        auto emitBufRef = LowerSchemaProvider::lowerSchema(BufferSize, outputSchema, MemoryLayoutType::ROW_LAYOUT);

        auto emitOp = EmitPhysicalOperator(emitHandlerId, std::move(emitBufRef));
        auto deltaOp = DeltaPhysicalOperator(
            std::move(deltaExprs),
            deltaHandlerId,
            std::move(layoutInfo.deltaFieldLayout),
            layoutInfo.deltaFieldsEntrySize,
            std::move(layoutInfo.fullRecordLayout),
            layoutInfo.fullRecordEntrySize);
        deltaOp.setChild(PhysicalOperator(emitOp));

        auto scanOp = ScanPhysicalOperator(scanBufRef, inputSchema.getFieldNames());
        scanOp.setChild(PhysicalOperator(deltaOp));

        auto pipeline = std::make_shared<Pipeline>(PhysicalOperator(scanOp));
        pipeline->getOperatorHandlers().emplace(emitHandlerId, std::make_shared<EmitOperatorHandler>());
        pipeline->getOperatorHandlers().emplace(deltaHandlerId, deltaHandler);

        auto options = nautilus::engine::Options{};
        options.setOption("engine.Compilation", isCompiled);
        options.setOption("engine.demangleFunctionNames", true);
        options.setOption("dump.all", true);
        auto compiledStage = std::make_shared<CompiledExecutablePipelineStage>(pipeline, pipeline->getOperatorHandlers(), options);

        MockedPipelineContext pec{bm};
        compiledStage->start(pec);
        for (auto& buffer : inputBuffers)
        {
            compiledStage->execute(buffer, pec);
        }
        compiledStage->stop(pec);

        return pec.emittedBuffers;
    }

    /// Single-buffer convenience overload.
    std::vector<TupleBuffer> runPipeline(
        const Schema& inputSchema, const Schema& outputSchema, std::vector<PhysicalDeltaExpression> deltaExprs, TupleBuffer& inputBuffer)
    {
        std::vector<TupleBuffer> buffers{inputBuffer};
        return runPipeline(inputSchema, outputSchema, std::move(deltaExprs), buffers);
    }

    /// Prepares an input TupleBuffer with sequence metadata.
    TupleBuffer createInputBuffer(SequenceNumber seqNum = SequenceNumber(SequenceNumber::INITIAL))
    {
        auto buffer = bm->getBufferBlocking();
        buffer.setSequenceNumber(seqNum);
        buffer.setChunkNumber(ChunkNumber(ChunkNumber::INITIAL));
        buffer.setLastChunk(true);
        return buffer;
    }

    static constexpr size_t BufferSize = 4096;
    std::shared_ptr<BufferManager> bm = BufferManager::create(BufferSize, 100);
};

INSTANTIATE_TEST_SUITE_P(Compilation, DeltaPhysicalOperatorTest, ::testing::Values(false, true));

/// A single record should be dropped because there is no previous value.
TEST_P(DeltaPhysicalOperatorTest, SingleRecordDropped)
{
    auto schema = Schema{}.addField("value", DataType::Type::INT64);
    Testing::TestTupleBuffer ttb(schema);

    auto inputBuffer = createInputBuffer();
    auto inputView = ttb.open(inputBuffer);
    inputView.append(10);

    auto results = runPipeline(
        schema, schema, {{PhysicalFunction(FieldAccessPhysicalFunction("value")), "value", {DataType::Type::INT64}}}, inputBuffer);

    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].getNumberOfTuples(), 0u);
}

/// Two records should produce one output with the correct delta.
TEST_P(DeltaPhysicalOperatorTest, TwoRecords)
{
    auto schema = Schema{}.addField("value", DataType::Type::UINT32);
    Testing::TestTupleBuffer ttb(schema);

    auto inputBuffer = createInputBuffer();
    auto inputView = ttb.open(inputBuffer);
    inputView.append(10);
    inputView.append(25);

    auto results = runPipeline(
        schema, schema, {{PhysicalFunction(FieldAccessPhysicalFunction("value")), "value", {DataType::Type::UINT32}}}, inputBuffer);

    ASSERT_EQ(results.size(), 1u);
    ASSERT_EQ(results[0].getNumberOfTuples(), 1u);
    auto outputView = ttb.open(results[0]);
    EXPECT_EQ(outputView[0]["value"].as<uint32_t>(), 15u); /// 25 - 10
}

/// N records should produce N-1 outputs with correct sequential deltas.
TEST_P(DeltaPhysicalOperatorTest, MultipleRecords)
{
    auto schema = Schema{}.addField("value", DataType::Type::INT64);
    Testing::TestTupleBuffer ttb(schema);

    auto inputBuffer = createInputBuffer();
    auto inputView = ttb.open(inputBuffer);
    inputView.append(10);
    inputView.append(25);
    inputView.append(20);
    inputView.append(50);

    auto results = runPipeline(
        schema, schema, {{PhysicalFunction(FieldAccessPhysicalFunction("value")), "value", {DataType::Type::INT64}}}, inputBuffer);

    ASSERT_EQ(results.size(), 1u);
    ASSERT_EQ(results[0].getNumberOfTuples(), 3u);
    auto outputView = ttb.open(results[0]);
    EXPECT_EQ(outputView[0]["value"].as<int64_t>(), 15); /// 25 - 10
    EXPECT_EQ(outputView[1]["value"].as<int64_t>(), -5); /// 20 - 25
    EXPECT_EQ(outputView[2]["value"].as<int64_t>(), 30); /// 50 - 20
}

/// With an alias, the delta is written to a new field while the original field is preserved.
TEST_P(DeltaPhysicalOperatorTest, WithAlias)
{
    auto inputSchema = Schema{}.addField("value", DataType::Type::INT64);
    auto outputSchema = Schema{}.addField("value", DataType::Type::INT64).addField("delta_value", DataType::Type::INT64);

    Testing::TestTupleBuffer inputTtb(inputSchema);
    auto inputBuffer = createInputBuffer();
    auto inputView = inputTtb.open(inputBuffer);
    inputView.append(10);
    inputView.append(25);

    auto results = runPipeline(
        inputSchema,
        outputSchema,
        {{PhysicalFunction(FieldAccessPhysicalFunction("value")), "delta_value", {DataType::Type::INT64}}},
        inputBuffer);

    ASSERT_EQ(results.size(), 1u);
    ASSERT_EQ(results[0].getNumberOfTuples(), 1u);

    Testing::TestTupleBuffer outputTtb(outputSchema);
    auto outputView = outputTtb.open(results[0]);
    EXPECT_EQ(outputView[0]["value"].as<int64_t>(), 25); /// original preserved
    EXPECT_EQ(outputView[0]["delta_value"].as<int64_t>(), 15); /// delta written to alias
}

/// Multiple fields are delta'd simultaneously.
TEST_P(DeltaPhysicalOperatorTest, MultipleDeltaExpressions)
{
    auto schema = Schema{}.addField("a", DataType::Type::INT64).addField("b", DataType::Type::INT64);
    Testing::TestTupleBuffer ttb(schema);

    auto inputBuffer = createInputBuffer();
    auto inputView = ttb.open(inputBuffer);
    inputView.append(10, 100);
    inputView.append(15, 80);
    inputView.append(25, 90);

    auto results = runPipeline(
        schema,
        schema,
        {
            {PhysicalFunction(FieldAccessPhysicalFunction("a")), "a", {DataType::Type::INT64}},
            {PhysicalFunction(FieldAccessPhysicalFunction("b")), "b", {DataType::Type::INT64}},
        },
        inputBuffer);

    ASSERT_EQ(results.size(), 1u);
    ASSERT_EQ(results[0].getNumberOfTuples(), 2u);
    auto outputView = ttb.open(results[0]);
    EXPECT_EQ(outputView[0]["a"].as<int64_t>(), 5); /// 15 - 10
    EXPECT_EQ(outputView[0]["b"].as<int64_t>(), -20); /// 80 - 100
    EXPECT_EQ(outputView[1]["a"].as<int64_t>(), 10); /// 25 - 15
    EXPECT_EQ(outputView[1]["b"].as<int64_t>(), 10); /// 90 - 80
}

/// Two sequential buffers: the handler bridges state so only the first record of the stream is dropped.
TEST_P(DeltaPhysicalOperatorTest, MultiBufferSequential)
{
    auto schema = Schema{}.addField("value", DataType::Type::INT64);
    Testing::TestTupleBuffer ttb(schema);

    /// Buffer 1 (seqNum=1): [10, 25]
    auto buf1 = createInputBuffer(SequenceNumber(1));
    {
        auto view = ttb.open(buf1);
        view.append(10);
        view.append(25);
    }

    /// Buffer 2 (seqNum=2): [30, 50]
    auto buf2 = createInputBuffer(SequenceNumber(2));
    {
        auto view = ttb.open(buf2);
        view.append(30);
        view.append(50);
    }

    std::vector<TupleBuffer> inputBuffers{buf1, buf2};
    auto results = runPipeline(
        schema, schema, {{PhysicalFunction(FieldAccessPhysicalFunction("value")), "value", {DataType::Type::INT64}}}, inputBuffers);

    /// Expected: 3 output records total (4 input - 1 dropped first of stream)
    /// Buffer 1: drop 10, emit delta(25-10)=15
    /// Buffer 2: delta(30-25)=5, delta(50-30)=20
    uint64_t totalRecords = 0;
    std::vector<int64_t> allValues;
    for (auto result : results)
    {
        auto view = ttb.open(result);
        for (uint64_t i = 0; i < result.getNumberOfTuples(); ++i)
        {
            allValues.push_back(view[i]["value"].as<int64_t>());
        }
        totalRecords += result.getNumberOfTuples();
    }

    EXPECT_EQ(totalRecords, 3u);
    ASSERT_EQ(allValues.size(), 3u);
    EXPECT_EQ(allValues[0], 15); /// 25 - 10
    EXPECT_EQ(allValues[1], 5); /// 30 - 25
    EXPECT_EQ(allValues[2], 20); /// 50 - 30
}

/// Two buffers processed in reverse order: the pending-first-record handshake emits the deferred record.
TEST_P(DeltaPhysicalOperatorTest, MultiBufferReverseOrder)
{
    auto schema = Schema{}.addField("value", DataType::Type::INT64);
    Testing::TestTupleBuffer ttb(schema);

    /// Buffer 1 (seqNum=1): [10, 25]
    auto buf1 = createInputBuffer(SequenceNumber(1));
    {
        auto view = ttb.open(buf1);
        view.append(10);
        view.append(25);
    }

    /// Buffer 2 (seqNum=2): [30, 50]
    auto buf2 = createInputBuffer(SequenceNumber(2));
    {
        auto view = ttb.open(buf2);
        view.append(30);
        view.append(50);
    }

    /// Process buffer 2 first, then buffer 1 (reverse order).
    std::vector<TupleBuffer> inputBuffers{buf2, buf1};
    auto results = runPipeline(
        schema, schema, {{PhysicalFunction(FieldAccessPhysicalFunction("value")), "value", {DataType::Type::INT64}}}, inputBuffers);

    /// Expected: 3 output records total
    /// Buffer 2 processed first: drop 30 (pending), emit delta(50-30)=20
    /// Buffer 1 processed second: drop 10 (first of stream), emit delta(25-10)=15,
    ///   then in close: finds pending record 30 from buf2, emits delta(30-25)=5
    uint64_t totalRecords = 0;
    std::vector<int64_t> allValues;
    for (auto result : results)
    {
        auto view = ttb.open(result);
        for (uint64_t i = 0; i < result.getNumberOfTuples(); ++i)
        {
            allValues.push_back(view[i]["value"].as<int64_t>());
        }
        totalRecords += result.getNumberOfTuples();
    }

    EXPECT_EQ(totalRecords, 3u);
    /// The values are the same set as sequential, but order depends on processing order.
    /// From buf2: 20 (50-30)
    /// From buf1: 15 (25-10), then 5 (30-25) emitted in close
    ASSERT_EQ(allValues.size(), 3u);
    EXPECT_EQ(allValues[0], 20); /// 50 - 30 (from buf2)
    EXPECT_EQ(allValues[1], 15); /// 25 - 10 (from buf1)
    EXPECT_EQ(allValues[2], 5); /// 30 - 25 (deferred from buf1's close)
}

}
