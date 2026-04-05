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

#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <MapPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <ScanPhysicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Nautilus/Interface/BufferRef/ArrowBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <folly/Synchronized.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <Pipeline.hpp>
#include <PipelineExecutionContext.hpp>
#include <TestTupleBuffer.hpp>
#include <options.hpp>

#include <arrow/api.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>

namespace NES
{

/// NOLINTBEGIN(readability-magic-numbers)

constexpr size_t defaultBufferSize = 8192;

/// Captures emitted buffers instead of sending to a real sink.
struct MockPipelineContext final : PipelineExecutionContext
{
    bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override { buffers.wlock()->emplace_back(buffer); return true; }
    TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }
    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return INITIAL<WorkerThreadId>; }
    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }
    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }
    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return *handlers; }
    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& h) override { handlers = &h; }
    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { INVARIANT(false, "Should not be called"); }
    TupleBuffer& pinBuffer(TupleBuffer&& tb) override { pinned.emplace_back(std::make_unique<TupleBuffer>(std::move(tb))); return *pinned.back(); }

    folly::Synchronized<std::vector<TupleBuffer>>& buffers;
    std::shared_ptr<BufferManager> bufferManager;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* handlers = nullptr;
    std::vector<std::unique_ptr<TupleBuffer>> pinned;

    MockPipelineContext(folly::Synchronized<std::vector<TupleBuffer>>& b, std::shared_ptr<BufferManager> bm)
        : buffers(b), bufferManager(std::move(bm)) {}
};

/// Parameterized on compiled (true) vs interpreted (false).
class ArrowBufferRefTest : public Testing::BaseUnitTest, public ::testing::WithParamInterface<bool>
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("ArrowBufferRefTest.log", LogLevel::LOG_DEBUG);
    }

    void SetUp() override { BaseUnitTest::SetUp(); }

    [[nodiscard]] bool isCompiled() const { return GetParam(); }

    /// Feeds `input` through a Scan(ROW) → Emit(Arrow) pipeline and returns the emitted buffers.
    template <typename Tuple>
    std::vector<TupleBuffer> runArrowPipeline(
        const std::vector<Tuple>& input,
        uint64_t inputBufferSize = defaultBufferSize,
        uint64_t emitBufferSize = defaultBufferSize)
    {
        auto schema = Testing::schemaOf<Tuple>();
        auto inputBufRef = LowerSchemaProvider::lowerSchema(inputBufferSize, schema, MemoryLayoutType::ROW_LAYOUT);
        auto outputBufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(emitBufferSize, schema, "Arrow", {});

        ScanPhysicalOperator scan(inputBufRef, schema.getFieldNames());
        EmitPhysicalOperator emit(OperatorHandlerId(0), outputBufRef);
        scan.setChild(PhysicalOperator(emit));

        auto pipeline = std::make_shared<Pipeline>(PhysicalOperator(scan));
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
        handlers[OperatorHandlerId(0)] = std::make_shared<EmitOperatorHandler>();

        nautilus::engine::Options opt;
        opt.setOption("engine.Compilation", isCompiled());
        CompiledExecutablePipelineStage stage(pipeline, handlers, opt);

        folly::Synchronized<std::vector<TupleBuffer>> emitted;
        emitted.wlock()->reserve(64);
        auto bufMgr = BufferManager::create(inputBufferSize, 200);
        MockPipelineContext pec{emitted, bufMgr};

        auto inputBuffer = bufMgr->getBufferBlocking();
        inputBuffer.setSequenceNumber(SequenceNumber(1));
        inputBuffer.setChunkNumber(ChunkNumber(1));
        inputBuffer.setLastChunk(true);
        inputBuffer.setOriginId(INITIAL<OriginId>);

        Testing::TestTupleBuffer ttb(schema);
        auto view = ttb.open(inputBuffer, bufMgr.get());
        for (const auto& row : input)
        {
            view.append(row);
        }

        stage.start(pec);
        stage.execute(inputBuffer, pec);
        stage.stop(pec);

        return std::move(*emitted.wlock());
    }

    /// Compares a single field against its expected value, using EXPECT_NEAR for floats.
    template <typename T>
    static void expectFieldEq(Testing::TestTupleBufferRecordView& rec, const std::string& name, const T& expected, size_t row)
    {
        if constexpr (std::is_same_v<T, float>)
        {
            EXPECT_NEAR(rec[name].template as<float>(), expected, 1e-5f) << "field=" << name << " row=" << row;
        }
        else if constexpr (std::is_same_v<T, double>)
        {
            EXPECT_NEAR(rec[name].template as<double>(), expected, 1e-5) << "field=" << name << " row=" << row;
        }
        else
        {
            EXPECT_EQ(rec[name].template as<T>(), expected) << "field=" << name << " row=" << row;
        }
    }

    /// Unwrap std::optional — nullopt means we expect a null field.
    template <typename T>
    static void expectFieldEq(Testing::TestTupleBufferRecordView& rec, const std::string& name, const std::optional<T>& expected, size_t row)
    {
        auto fieldValue = rec[name].get();
        if (!expected.has_value())
        {
            EXPECT_TRUE(std::holds_alternative<std::monostate>(fieldValue)) << "field=" << name << " row=" << row << " expected null";
            return;
        }
        EXPECT_FALSE(std::holds_alternative<std::monostate>(fieldValue)) << "field=" << name << " row=" << row << " got null, expected value";
        expectFieldEq(rec, name, expected.value(), row);
    }

    template <typename Tuple, size_t... Is>
    static void expectTupleEq(Testing::TestTupleBufferRecordView& rec, const Tuple& expected, size_t row, std::index_sequence<Is...>)
    {
        (expectFieldEq(rec, "f" + std::to_string(Is), std::get<Is>(expected), row), ...);
    }

    /// Reads all records from Arrow-layout buffers and compares them against the expected input.
    template <typename Tuple>
    void verifyArrowOutput(
        const std::vector<TupleBuffer>& buffers,
        uint64_t emitBufferSize,
        const std::vector<Tuple>& expected)
    {
        auto schema = Testing::schemaOf<Tuple>();
        auto bufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(emitBufferSize, schema, "Arrow", {});

        size_t globalIdx = 0;
        for (auto& buf : buffers)
        {
            auto mutableBuf = buf;
            Testing::TestTupleBuffer ttb(schema);
            auto view = ttb.openWithBufferRef(mutableBuf, bufRef);
            for (size_t i = 0; i < view.getNumberOfTuples(); ++i)
            {
                ASSERT_LT(globalIdx, expected.size()) << "More output records than expected";
                auto rec = view[i];
                expectTupleEq(rec, expected[globalIdx], globalIdx, std::make_index_sequence<std::tuple_size_v<Tuple>>{});
                ++globalIdx;
            }
        }
        EXPECT_EQ(globalIdx, expected.size()) << "Record count mismatch";
    }

    /// Maps a NES DataType to an Arrow data type.
    static std::shared_ptr<arrow::DataType> toArrowType(const DataType::Type type)
    {
        switch (type)
        {
            case DataType::Type::INT8:    return arrow::int8();
            case DataType::Type::INT16:   return arrow::int16();
            case DataType::Type::INT32:   return arrow::int32();
            case DataType::Type::INT64:   return arrow::int64();
            case DataType::Type::UINT8:   return arrow::uint8();
            case DataType::Type::UINT16:  return arrow::uint16();
            case DataType::Type::UINT32:  return arrow::uint32();
            case DataType::Type::UINT64:  return arrow::uint64();
            case DataType::Type::FLOAT32: return arrow::float32();
            case DataType::Type::FLOAT64: return arrow::float64();
            case DataType::Type::BOOLEAN: return arrow::boolean();
            case DataType::Type::CHAR:    return arrow::uint8();
            case DataType::Type::VARSIZED: return arrow::utf8();
            default: return nullptr;
        }
    }

    /// Wraps a TupleBuffer (written in Arrow layout) as an arrow::RecordBatch and validates it.
    /// Uses ArrowBufferRef for layout info and child buffer collection to stay in sync with the actual sink code.
    static std::shared_ptr<arrow::RecordBatch> wrapAsArrowRecordBatch(const TupleBuffer& buffer, const Schema& schema)
    {
        const auto numRows = static_cast<int64_t>(buffer.getNumberOfTuples());

        auto bufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(buffer.getBufferSize(), schema, "Arrow", {});
        auto arrowBufRef = std::dynamic_pointer_cast<const ArrowBufferRef>(bufRef);
        EXPECT_TRUE(arrowBufRef != nullptr);

        const auto& fields = arrowBufRef->getFields();
        const auto cap = arrowBufRef->getCapacity();
        const uint64_t bitmapBytesPerColumn = ArrowBufferRef::align8((cap + 7) / 8);

        arrow::FieldVector arrowFields;
        for (const auto& field : schema)
        {
            arrowFields.push_back(arrow::field(field.name, toArrowType(field.dataType.type), field.dataType.nullable));
        }
        auto arrowSchema = arrow::schema(std::move(arrowFields));

        auto* rawBuffer = buffer.getAvailableMemoryArea<uint8_t>().data();
        std::vector<std::shared_ptr<arrow::Array>> arrays;

        for (size_t i = 0; i < fields.size(); ++i)
        {
            const auto& field = fields[i];
            const auto arrowType = toArrowType(field.type.type);

            std::shared_ptr<arrow::Buffer> nullBitmapBuf;
            if (field.type.nullable)
            {
                nullBitmapBuf = arrow::Buffer::Wrap(rawBuffer + field.bitmapOffset, bitmapBytesPerColumn);
            }

            if (field.type.type == DataType::Type::VARSIZED)
            {
                const uint64_t offsetsBufSize = static_cast<uint64_t>(cap + 1) * sizeof(int32_t);
                auto offsetsBuf = arrow::Buffer::Wrap(rawBuffer + field.dataOffset, offsetsBufSize);

                auto childBuffers = arrowBufRef->loadVarSizedColumnChildBuffers(buffer, i);

                uint64_t totalDataBytes = 0;
                for (const auto& cb : childBuffers)
                {
                    totalDataBytes += cb.getNumberOfTuples();
                }

                std::shared_ptr<arrow::Buffer> dataBuf;
                if (childBuffers.size() == 1)
                {
                    dataBuf = arrow::Buffer::Wrap(childBuffers[0].getAvailableMemoryArea<uint8_t>().data(), totalDataBytes);
                }
                else if (childBuffers.size() > 1)
                {
                    auto allocResult = arrow::AllocateBuffer(static_cast<int64_t>(totalDataBytes));
                    EXPECT_TRUE(allocResult.ok());
                    auto arrowBuf = std::move(*allocResult);
                    auto* dest = arrowBuf->mutable_data();
                    uint64_t pos = 0;
                    for (const auto& cb : childBuffers)
                    {
                        const auto usedBytes = cb.getNumberOfTuples();
                        std::memcpy(dest + pos, cb.getAvailableMemoryArea<uint8_t>().data(), usedBytes);
                        pos += usedBytes;
                    }
                    dataBuf = std::move(arrowBuf);
                }
                else
                {
                    dataBuf = arrow::Buffer::Wrap(rawBuffer, 0);
                }

                auto arrayData = arrow::ArrayData::Make(arrowType, numRows, {nullBitmapBuf, offsetsBuf, dataBuf});
                arrays.push_back(arrow::MakeArray(arrayData));
            }
            else if (field.type.type == DataType::Type::BOOLEAN)
            {
                /// BOOLEAN: data is already bit-packed in the buffer — wrap directly.
                const uint64_t dataBufSize = ArrowBufferRef::align8((cap + 7) / 8);
                auto dataBuf = arrow::Buffer::Wrap(rawBuffer + field.dataOffset, dataBufSize);
                auto arrayData = arrow::ArrayData::Make(arrowType, numRows, {nullBitmapBuf, dataBuf});
                arrays.push_back(arrow::MakeArray(arrayData));
            }
            else
            {
                const uint64_t dataBufSize = cap * field.dataTypeSize;
                auto dataBuf = arrow::Buffer::Wrap(rawBuffer + field.dataOffset, dataBufSize);
                auto arrayData = arrow::ArrayData::Make(arrowType, numRows, {nullBitmapBuf, dataBuf});
                arrays.push_back(arrow::MakeArray(arrayData));
            }
        }
        return arrow::RecordBatch::Make(arrowSchema, numRows, std::move(arrays));
    }

    /// Wraps each emitted buffer as an Arrow RecordBatch and validates it passes Arrow's internal consistency checks.
    static void verifyArrowFormat(const std::vector<TupleBuffer>& buffers, const Schema& schema)
    {
        for (const auto& buf : buffers)
        {
            if (buf.getNumberOfTuples() == 0)
            {
                continue;
            }
            auto batch = wrapAsArrowRecordBatch(buf, schema);
            ASSERT_NE(batch, nullptr);
            auto status = batch->Validate();
            EXPECT_TRUE(status.ok()) << "Arrow RecordBatch validation failed: " << status.ToString();
        }
    }
};

INSTANTIATE_TEST_SUITE_P(ExecutionMode, ArrowBufferRefTest, ::testing::Values(false, true),
    [](const auto& info) { return info.param ? "Compiled" : "Interpreter"; });

/// Mixed types: int32, nullable float (with actual nulls), string (VARSIZED).
TEST_P(ArrowBufferRefTest, MixedTypesWithNullableAndVarSized)
{
    using Row = std::tuple<int32_t, std::optional<float>, std::string>;
    std::vector<Row> input = {
        {1, 3.14f, "hello"},
        {-42, std::nullopt, "world"},
        {100, 99.9f, "nebulastream"},
        {0, std::nullopt, ""},
    };

    auto buffers = runArrowPipeline(input);
    ASSERT_FALSE(buffers.empty());
    verifyArrowOutput(buffers, defaultBufferSize, input);
    verifyArrowFormat(buffers, Testing::schemaOf<Row>());
}

/// All fixed-width types.
TEST_P(ArrowBufferRefTest, AllFixedWidthTypes)
{
    using Row = std::tuple<uint8_t, uint32_t, uint64_t, int8_t, int64_t, float, double>;
    std::vector<Row> input = {{42, 1000, 999999, -5, -123456, 3.14f, 2.718}, {0, 0, 0, 0, 0, 0.0f, 0.0}};

    auto buffers = runArrowPipeline(input);
    ASSERT_FALSE(buffers.empty());
    verifyArrowOutput(buffers, defaultBufferSize, input);
    verifyArrowFormat(buffers, Testing::schemaOf<Row>());
}

/// Small emit buffer forces multiple output buffers.
TEST_P(ArrowBufferRefTest, SmallEmitBufferProducesMultipleOutputs)
{
    using Row = std::tuple<uint64_t>;
    std::vector<Row> input;
    for (uint64_t i = 0; i < 50; ++i)
    {
        input.emplace_back(i * 100);
    }

    constexpr uint64_t smallEmit = 128;
    auto buffers = runArrowPipeline(input, defaultBufferSize, smallEmit);
    ASSERT_GT(buffers.size(), 1U) << "Expected multiple output buffers";
    verifyArrowOutput(buffers, smallEmit, input);
    verifyArrowFormat(buffers, Testing::schemaOf<Row>());
}

/// Empty input produces no output.
TEST_P(ArrowBufferRefTest, EmptyInput)
{
    using Row = std::tuple<uint64_t>;
    auto buffers = runArrowPipeline(std::vector<Row>{});
    verifyArrowOutput(buffers, defaultBufferSize, std::vector<Row>{});
}

/// Large strings (10K+ characters) verify child buffer handling and Arrow offset correctness.
TEST_P(ArrowBufferRefTest, LargeStrings)
{
    using Row = std::tuple<int32_t, std::string>;
    const std::string bigA(10000, 'A');
    const std::string bigB(10000, 'B');
    const std::string bigC(15000, 'C');
    std::vector<Row> input = {
        {1, bigA},
        {2, bigB},
        {3, bigC},
        {4, "short"},
        {5, ""},
    };

    constexpr uint64_t largeBuf = 65536;
    auto buffers = runArrowPipeline(input, largeBuf, largeBuf);
    ASSERT_FALSE(buffers.empty());
    verifyArrowOutput(buffers, largeBuf, input);
    verifyArrowFormat(buffers, Testing::schemaOf<Row>());
}

/// Multiple large strings that exceed child buffer capacity — verifies child buffer overflow handling.
TEST_P(ArrowBufferRefTest, LargeStringsMultipleChildBuffers)
{
    using Row = std::tuple<uint64_t, std::string>;
    std::vector<Row> input;
    for (uint64_t i = 0; i < 20; ++i)
    {
        input.emplace_back(i, std::string(10000, static_cast<char>('a' + i % 26)));
    }

    /// 65536 buffer size, enough offsets capacity for all 20 records.
    /// Each child buffer is 65536 bytes, so ~6 strings fill one child buffer → multiple child buffers needed.
    constexpr uint64_t bufSize = 65536;
    auto buffers = runArrowPipeline(input, bufSize, bufSize);
    ASSERT_FALSE(buffers.empty());
    verifyArrowOutput(buffers, bufSize, input);
    verifyArrowFormat(buffers, Testing::schemaOf<Row>());
}

/// A single string larger than the child buffer size must not crash.
TEST_P(ArrowBufferRefTest, StringLargerThanChildBuffer)
{
    using Row = std::tuple<int32_t, std::string>;
    /// Buffer size is 8192, so child buffers are 8192 bytes. A 20K string exceeds that.
    const std::string huge(20000, 'X');
    std::vector<Row> input = {{1, huge}, {2, "small"}};

    auto buffers = runArrowPipeline(input);
    ASSERT_FALSE(buffers.empty());
    verifyArrowOutput(buffers, defaultBufferSize, input);
    verifyArrowFormat(buffers, Testing::schemaOf<Row>());
}

/// Nullable VARSIZED fields: null strings must not crash and must roundtrip correctly.
TEST_P(ArrowBufferRefTest, NullableVarSized)
{
    using Row = std::tuple<int32_t, std::optional<std::string>>;
    std::vector<Row> input = {
        {1, "hello"},
        {2, std::nullopt},
        {3, "world"},
        {4, std::nullopt},
        {5, ""},
    };

    auto buffers = runArrowPipeline(input);
    ASSERT_FALSE(buffers.empty());
    verifyArrowOutput(buffers, defaultBufferSize, input);
    verifyArrowFormat(buffers, Testing::schemaOf<Row>());
}

/// Two VARSIZED columns with different overflow patterns — tests interleaved child buffer stride logic.
TEST_P(ArrowBufferRefTest, MultipleVarSizedColumns)
{
    using Row = std::tuple<int32_t, std::string, std::string>;
    std::vector<Row> input;
    for (int32_t i = 0; i < 15; ++i)
    {
        /// Column 1: large strings that will overflow the child buffer
        /// Column 2: small strings that fit in a single child buffer
        input.emplace_back(i, std::string(5000, static_cast<char>('A' + i % 26)), std::string(10, static_cast<char>('z' - i % 26)));
    }

    constexpr uint64_t bufSize = 65536;
    auto buffers = runArrowPipeline(input, bufSize, bufSize);
    ASSERT_FALSE(buffers.empty());
    verifyArrowOutput(buffers, bufSize, input);
    verifyArrowFormat(buffers, Testing::schemaOf<Row>());
}

/// All-null VARSIZED column — no child buffer is ever allocated.
TEST_P(ArrowBufferRefTest, AllNullVarSized)
{
    using Row = std::tuple<int32_t, std::optional<std::string>>;
    std::vector<Row> input = {
        {1, std::nullopt},
        {2, std::nullopt},
        {3, std::nullopt},
    };

    auto buffers = runArrowPipeline(input);
    ASSERT_FALSE(buffers.empty());
    verifyArrowOutput(buffers, defaultBufferSize, input);
    verifyArrowFormat(buffers, Testing::schemaOf<Row>());
}

/// Empty string vs null string must be distinguishable through Arrow wrapping.
TEST_P(ArrowBufferRefTest, EmptyStringVsNullString)
{
    using Row = std::tuple<int32_t, std::optional<std::string>>;
    std::vector<Row> input = {
        {1, ""},           // valid, zero-length
        {2, std::nullopt}, // null
        {3, ""},           // valid, zero-length
        {4, "data"},       // valid, non-empty
        {5, std::nullopt}, // null
    };

    auto buffers = runArrowPipeline(input);
    ASSERT_FALSE(buffers.empty());
    verifyArrowOutput(buffers, defaultBufferSize, input);

    /// Additionally verify Arrow-level null vs empty distinction
    auto schema = Testing::schemaOf<Row>();
    for (const auto& buf : buffers)
    {
        if (buf.getNumberOfTuples() == 0)
        {
            continue;
        }
        auto batch = wrapAsArrowRecordBatch(buf, schema);
        ASSERT_NE(batch, nullptr);
        auto status = batch->Validate();
        ASSERT_TRUE(status.ok()) << status.ToString();

        auto strArray = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
        /// Row 0: empty string — valid, length 0
        EXPECT_TRUE(strArray->IsValid(0)) << "row 0 should be valid (empty string)";
        EXPECT_EQ(strArray->GetView(0).length(), 0) << "row 0 should be zero-length";
        /// Row 1: null
        EXPECT_TRUE(strArray->IsNull(1)) << "row 1 should be null";
        /// Row 2: empty string — valid, length 0
        EXPECT_TRUE(strArray->IsValid(2)) << "row 2 should be valid (empty string)";
        EXPECT_EQ(strArray->GetView(2).length(), 0) << "row 2 should be zero-length";
        /// Row 3: "data" — valid, length 4
        EXPECT_TRUE(strArray->IsValid(3)) << "row 3 should be valid";
        EXPECT_EQ(strArray->GetView(3), "data") << "row 3 content mismatch";
        /// Row 4: null
        EXPECT_TRUE(strArray->IsNull(4)) << "row 4 should be null";
    }
}

/// Nullable VARSIZED Arrow wrapping — verify null bitmap is correct in the Arrow RecordBatch.
TEST_P(ArrowBufferRefTest, NullableVarSizedArrowBitmap)
{
    using Row = std::tuple<std::optional<std::string>, std::optional<int32_t>>;
    std::vector<Row> input = {
        {"hello", 42},
        {std::nullopt, std::nullopt},
        {"world", 100},
        {std::nullopt, 7},
        {"test", std::nullopt},
    };

    auto buffers = runArrowPipeline(input);
    ASSERT_FALSE(buffers.empty());
    verifyArrowOutput(buffers, defaultBufferSize, input);

    auto schema = Testing::schemaOf<Row>();
    for (const auto& buf : buffers)
    {
        if (buf.getNumberOfTuples() == 0)
        {
            continue;
        }
        auto batch = wrapAsArrowRecordBatch(buf, schema);
        ASSERT_NE(batch, nullptr);
        auto status = batch->Validate();
        ASSERT_TRUE(status.ok()) << status.ToString();

        /// Verify string column (col 0) null bitmap
        auto strCol = batch->column(0);
        EXPECT_TRUE(strCol->IsValid(0));
        EXPECT_TRUE(strCol->IsNull(1));
        EXPECT_TRUE(strCol->IsValid(2));
        EXPECT_TRUE(strCol->IsNull(3));
        EXPECT_TRUE(strCol->IsValid(4));

        /// Verify int32 column (col 1) null bitmap
        auto intCol = batch->column(1);
        EXPECT_TRUE(intCol->IsValid(0));
        EXPECT_TRUE(intCol->IsNull(1));
        EXPECT_TRUE(intCol->IsValid(2));
        EXPECT_TRUE(intCol->IsValid(3));
        EXPECT_TRUE(intCol->IsNull(4));

        /// Verify string values
        auto strArray = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
        EXPECT_EQ(strArray->GetView(0), "hello");
        EXPECT_EQ(strArray->GetView(2), "world");
        EXPECT_EQ(strArray->GetView(4), "test");

        /// Verify int32 values
        auto intArray = std::static_pointer_cast<arrow::Int32Array>(batch->column(1));
        EXPECT_EQ(intArray->Value(0), 42);
        EXPECT_EQ(intArray->Value(2), 100);
        EXPECT_EQ(intArray->Value(3), 7);
    }
}

/// Boolean column with 100 rows — Scan(ROW) → Emit(Arrow).
TEST_P(ArrowBufferRefTest, BooleanColumnRowToArrow)
{
    using Row = std::tuple<uint64_t, bool>;
    std::vector<Row> input;
    for (uint64_t i = 0; i < 100; ++i)
    {
        input.emplace_back(i, i % 2 == 0);
    }

    auto buffers = runArrowPipeline(input);
    ASSERT_FALSE(buffers.empty());
    verifyArrowOutput(buffers, defaultBufferSize, input);
    verifyArrowFormat(buffers, Testing::schemaOf<Row>());
}

/// Boolean column through Scan(Arrow) → Map → Emit(Arrow) compiled pipeline.
/// Uses the same 14-column schema and 4096 buffer size as the PyArrow interop e2e test,
/// which gives capacity=62 rows per buffer — matching the conditions where the e2e test
/// shows boolean corruption beyond the 8th row per output buffer.
TEST_P(ArrowBufferRefTest, BooleanColumnArrowToArrow)
{
    using Row = std::tuple<uint64_t, int8_t, int16_t, int32_t, int64_t,
                           uint8_t, uint16_t, uint32_t, uint64_t,
                           float, double, bool,
                           std::optional<int32_t>, std::optional<double>>;
    constexpr uint64_t numRows = 100;
    constexpr uint64_t bufSize = 4096;

    std::vector<Row> input;
    for (uint64_t i = 0; i < numRows; ++i)
    {
        input.emplace_back(
            i,
            static_cast<int8_t>(i % 127),
            static_cast<int16_t>(i % 32000),
            static_cast<int32_t>(i * 10),
            static_cast<int64_t>(i * 100),
            static_cast<uint8_t>(i % 255),
            static_cast<uint16_t>(i % 60000),
            static_cast<uint32_t>(i * 1000),
            i * 10000,
            static_cast<float>(i) * 0.5f,
            static_cast<double>(i) * 1.5,
            i % 2 == 0,
            i % 5 == 0 ? std::nullopt : std::optional<int32_t>(static_cast<int32_t>(i * 10)),
            i % 5 == 0 ? std::nullopt : std::optional<double>(static_cast<double>(i) * 1.5));
    }

    auto schema = Testing::schemaOf<Row>();
    auto arrowBufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(bufSize, schema, "Arrow", {});

    auto bufMgr = BufferManager::create(bufSize, 200);
    auto inputBuffer = bufMgr->getBufferBlocking();
    inputBuffer.setSequenceNumber(SequenceNumber(1));
    inputBuffer.setChunkNumber(ChunkNumber(1));
    inputBuffer.setLastChunk(true);
    inputBuffer.setOriginId(INITIAL<OriginId>);

    Testing::TestTupleBuffer ttbIn(schema);
    auto viewIn = ttbIn.openWithBufferRef(inputBuffer, arrowBufRef, bufMgr.get());
    /// Capacity is ~62 rows with 14 columns at 4096 bytes — fill up to capacity
    const auto cap = arrowBufRef->getCapacity();
    const auto rowsInFirstBuffer = std::min(numRows, cap);
    for (uint64_t i = 0; i < rowsInFirstBuffer; ++i)
    {
        std::apply([&](auto&&... args) { viewIn.append(std::forward<decltype(args)>(args)...); }, input[i]);
    }

    /// Scan(Arrow) → Map(f0..f13) → Emit(Arrow) — matches LowerToPhysicalProjection
    ScanPhysicalOperator scan(arrowBufRef, schema.getFieldNames());
    EmitPhysicalOperator emit(OperatorHandlerId(0), arrowBufRef);

    auto fieldNames = schema.getFieldNames();
    PhysicalOperator current(emit);
    for (auto it = fieldNames.rbegin(); it != fieldNames.rend(); ++it)
    {
        MapPhysicalOperator map(*it, PhysicalFunction(FieldAccessPhysicalFunction(*it)));
        map.setChild(current);
        current = PhysicalOperator(map);
    }
    scan.setChild(current);

    auto pipeline = std::make_shared<Pipeline>(PhysicalOperator(scan));
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
    handlers[OperatorHandlerId(0)] = std::make_shared<EmitOperatorHandler>();

    nautilus::engine::Options opt;
    opt.setOption("engine.Compilation", isCompiled());
    CompiledExecutablePipelineStage stage(pipeline, handlers, opt);

    folly::Synchronized<std::vector<TupleBuffer>> emitted;
    MockPipelineContext pec{emitted, bufMgr};

    stage.start(pec);
    stage.execute(inputBuffer, pec);
    stage.stop(pec);

    auto outputBuffers = std::move(*emitted.wlock());
    ASSERT_FALSE(outputBuffers.empty());

    std::vector<Row> expected(input.begin(), input.begin() + static_cast<long>(rowsInFirstBuffer));
    verifyArrowOutput(outputBuffers, bufSize, expected);
}

/// Boolean multi-buffer test: calls execute() multiple times on the SAME compiled pipeline,
/// simulating how SourceThread feeds multiple source buffers through the scan→emit pipeline.
/// Fills input buffers with RAW memcpy/loop like ArrowFileSource does (not via ArrowBufferRef::writeRecord).
TEST_P(ArrowBufferRefTest, BooleanMultiBufferArrowToArrow)
{
    using Row = std::tuple<uint64_t, bool>;
    constexpr uint64_t bufSize = 4096;

    auto schema = Testing::schemaOf<Row>();
    auto arrowBufRef = std::dynamic_pointer_cast<const ArrowBufferRef>(
        LowerSchemaProvider::lowerSchemaWithOutputFormat(bufSize, schema, "Arrow", {}));
    ASSERT_TRUE(arrowBufRef);
    const auto cap = arrowBufRef->getCapacity();
    ASSERT_GT(cap, 100) << "Capacity too small for this test";

    const uint64_t totalRows = cap * 2 + cap / 2;
    std::vector<Row> allInput;
    for (uint64_t i = 0; i < totalRows; ++i)
    {
        allInput.emplace_back(i, i % 2 == 0);
    }

    auto bufMgr = BufferManager::create(bufSize, 8);

    /// Build pipeline: Scan(Arrow) → Map(f0) → Map(f1) → Emit(Arrow)
    ScanPhysicalOperator scan(std::const_pointer_cast<TupleBufferRef>(std::static_pointer_cast<const TupleBufferRef>(arrowBufRef)),
                              schema.getFieldNames());
    EmitPhysicalOperator emit(OperatorHandlerId(0),
                              std::const_pointer_cast<TupleBufferRef>(std::static_pointer_cast<const TupleBufferRef>(arrowBufRef)));

    auto fieldNames = schema.getFieldNames();
    PhysicalOperator current(emit);
    for (auto it = fieldNames.rbegin(); it != fieldNames.rend(); ++it)
    {
        MapPhysicalOperator map(*it, PhysicalFunction(FieldAccessPhysicalFunction(*it)));
        map.setChild(current);
        current = PhysicalOperator(map);
    }
    scan.setChild(current);

    auto pipeline = std::make_shared<Pipeline>(PhysicalOperator(scan));
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
    handlers[OperatorHandlerId(0)] = std::make_shared<EmitOperatorHandler>();

    nautilus::engine::Options opt;
    opt.setOption("engine.Compilation", isCompiled());
    CompiledExecutablePipelineStage stage(pipeline, handlers, opt);

    folly::Synchronized<std::vector<TupleBuffer>> emitted;
    MockPipelineContext pec{emitted, bufMgr};

    stage.start(pec);

    /// Fill input buffers using raw memcpy exactly like ArrowFileSource does
    const auto& fields = arrowBufRef->getFields();
    uint64_t rowOffset = 0;
    uint64_t bufferIdx = 0;
    while (rowOffset < totalRows)
    {
        const uint64_t rowsThisBatch = std::min(cap, totalRows - rowOffset);

        auto inputBuffer = bufMgr->getBufferBlocking();
        inputBuffer.setSequenceNumber(SequenceNumber(bufferIdx + 1));
        inputBuffer.setChunkNumber(ChunkNumber(1));
        inputBuffer.setLastChunk(true);
        inputBuffer.setOriginId(INITIAL<OriginId>);

        auto* rawBuf = inputBuffer.getAvailableMemoryArea<uint8_t>().data();
        /// Do NOT zero the buffer — ArrowFileSource doesn't either, and recycled buffers may have old data

        /// Write bitmap (all valid for non-nullable)
        for (const auto& field : fields)
        {
            auto* bitmapDst = rawBuf + field.bitmapOffset;
            for (uint64_t r = 0; r < rowsThisBatch; ++r)
            {
                arrow::bit_util::SetBit(bitmapDst, r);
            }
        }

        /// Write uint64 data column (field 0)
        {
            auto* dataDst = reinterpret_cast<uint64_t*>(rawBuf + fields[0].dataOffset);
            for (uint64_t r = 0; r < rowsThisBatch; ++r)
            {
                dataDst[r] = rowOffset + r;
            }
        }

        /// Write boolean data column (field 1) — bit-packed like ArrowFileSource
        {
            auto* dataDst = rawBuf + fields[1].dataOffset;
            for (uint64_t r = 0; r < rowsThisBatch; ++r)
            {
                if ((rowOffset + r) % 2 == 0)
                    arrow::bit_util::SetBit(dataDst, r);
                else
                    arrow::bit_util::ClearBit(dataDst, r);
            }
        }

        inputBuffer.setNumberOfTuples(rowsThisBatch);

        stage.execute(inputBuffer, pec);
        rowOffset += rowsThisBatch;
        ++bufferIdx;
    }

    stage.stop(pec);

    auto outputBuffers = std::move(*emitted.wlock());
    ASSERT_FALSE(outputBuffers.empty()) << "No output buffers emitted";

    verifyArrowOutput(outputBuffers, bufSize, allInput);
}

/// NOLINTEND(readability-magic-numbers)

}
