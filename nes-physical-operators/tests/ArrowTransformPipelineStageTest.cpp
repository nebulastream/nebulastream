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

#include <ArrowTransformPipelineStage.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Interface/BufferRef/ArrowCData.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <Pipeline.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <ScanPhysicalOperator.hpp>
#include <TestTupleBuffer.hpp>
#include <options.hpp>

/// NOLINTBEGIN(readability-magic-numbers, cppcoreguidelines-pro-bounds-pointer-arithmetic, cppcoreguidelines-owning-memory)
namespace NES
{
namespace
{
constexpr uint64_t BUFFER_SIZE = 4096;

struct CollectingContext final : PipelineExecutionContext
{
    explicit CollectingContext(std::shared_ptr<BufferManager> bufferManager) : bufferManager(std::move(bufferManager)) { }

    bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
    {
        emitted.push_back(buffer);
        return true;
    }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { INVARIANT(false, "Not expected"); }

    TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return INITIAL<WorkerThreadId>; }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return *handlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& newHandlers) override
    {
        handlers = &newHandlers;
    }

    std::shared_ptr<BufferManager> bufferManager;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* handlers = nullptr;
    std::vector<TupleBuffer> emitted;
};

/// A batch owned by the "foreign" side of the interface, built only from the Arrow ABI like a Python operator would.
/// Columns: id INT64 (not null), value nullable FLOAT64, flag BOOLEAN, name nullable VARSIZED
struct ForeignBatch
{
    struct Column
    {
        std::vector<uint8_t> validity;
        std::vector<uint8_t> fixed;
        std::vector<int32_t> offsets;
        std::string payload;
        std::array<const void*, 3> buffers{};
        ArrowArray array{};
        ArrowSchema schema{};
    };

    std::array<Column, 4> columns;
    std::array<ArrowArray*, 4> arrayChildren{};
    std::array<ArrowSchema*, 4> schemaChildren{};
    std::array<const void*, 1> structBuffers{nullptr};
};

struct Row
{
    int64_t id;
    std::optional<double> value;
    bool flag;
    std::optional<std::string> name;

    bool operator==(const Row&) const = default;
};

void setBit(std::vector<uint8_t>& bitmap, const size_t index, const bool value)
{
    if (value)
    {
        bitmap[index / 8] |= static_cast<uint8_t>(1U << (index % 8));
    }
}

bool getBit(const void* bitmap, const size_t index)
{
    return ((static_cast<const uint8_t*>(bitmap)[index / 8] >> (index % 8)) & 1U) != 0;
}

/// Reads the batch purely through the ABI
std::vector<Row> readForeign(const ArrowArray& array)
{
    std::vector<Row> rows;
    for (int64_t logical = 0; logical < array.length; ++logical)
    {
        const auto at = array.offset + logical;
        const auto* id = array.children[0];
        const auto* value = array.children[1];
        const auto* flag = array.children[2];
        const auto* name = array.children[3];
        Row row{};
        row.id = static_cast<const int64_t*>(id->buffers[1])[id->offset + at];
        if (value->buffers[0] == nullptr || getBit(value->buffers[0], value->offset + at))
        {
            row.value = static_cast<const double*>(value->buffers[1])[value->offset + at];
        }
        row.flag = getBit(flag->buffers[1], flag->offset + at);
        if (name->buffers[0] == nullptr || getBit(name->buffers[0], name->offset + at))
        {
            const auto* offsets = static_cast<const int32_t*>(name->buffers[1]) + name->offset + at;
            row.name = std::string{static_cast<const char*>(name->buffers[2]) + offsets[0], static_cast<size_t>(offsets[1] - offsets[0])};
        }
        rows.push_back(row);
    }
    return rows;
}

/// Builds a heap-owned batch. @param leadingGarbage rows are prepended and hidden via the offset fields, which is what
/// slicing in pyarrow produces and what a consumer has to honor.
void writeForeign(const std::vector<Row>& logicalRows, const size_t leadingGarbage, ArrowArray& outArray, ArrowSchema& outSchema)
{
    std::vector<Row> rows(leadingGarbage, Row{-1, std::nullopt, true, "garbage"});
    rows.insert(rows.end(), logicalRows.begin(), logicalRows.end());
    const auto bitmapBytes = (rows.size() / 8) + 1;

    auto* batch = new ForeignBatch;
    const std::array formats{"l", "g", "b", "u"};
    const std::array names{"id", "value", "flag", "name"};
    auto& [id, value, flag, name] = batch->columns;
    id.fixed.resize(rows.size() * sizeof(int64_t));
    value.fixed.resize(rows.size() * sizeof(double));
    value.validity.resize(bitmapBytes);
    flag.fixed.resize(bitmapBytes);
    name.validity.resize(bitmapBytes);
    name.offsets.push_back(0);
    for (size_t i = 0; i < rows.size(); ++i)
    {
        reinterpret_cast<int64_t*>(id.fixed.data())[i] = rows[i].id;
        reinterpret_cast<double*>(value.fixed.data())[i] = rows[i].value.value_or(0);
        setBit(value.validity, i, rows[i].value.has_value());
        setBit(flag.fixed, i, rows[i].flag);
        setBit(name.validity, i, rows[i].name.has_value());
        name.payload += rows[i].name.value_or("");
        name.offsets.push_back(static_cast<int32_t>(name.payload.size()));
    }
    id.buffers = {nullptr, id.fixed.data(), nullptr};
    value.buffers = {value.validity.data(), value.fixed.data(), nullptr};
    flag.buffers = {nullptr, flag.fixed.data(), nullptr};
    name.buffers = {name.validity.data(), name.offsets.data(), name.payload.data()};

    for (size_t i = 0; i < batch->columns.size(); ++i)
    {
        auto& column = batch->columns[i];
        /// Children are owned by the parent. A no-op release is valid because this test never moves them out.
        column.array = ArrowArray{
            .length = static_cast<int64_t>(rows.size()),
            .null_count = (i == 1 || i == 3) ? -1 : 0,
            .offset = 0,
            .n_buffers = i == 3 ? 3 : 2,
            .n_children = 0,
            .buffers = column.buffers.data(),
            .children = nullptr,
            .dictionary = nullptr,
            .release = +[](ArrowArray* self) { self->release = nullptr; },
            .private_data = nullptr};
        column.schema = ArrowSchema{
            .format = formats[i],
            .name = names[i],
            .metadata = nullptr,
            .flags = 0,
            .n_children = 0,
            .children = nullptr,
            .dictionary = nullptr,
            .release = +[](ArrowSchema* self) { self->release = nullptr; },
            .private_data = nullptr};
        batch->arrayChildren[i] = &column.array;
        batch->schemaChildren[i] = &column.schema;
    }
    outArray = ArrowArray{
        .length = static_cast<int64_t>(logicalRows.size()),
        .null_count = 0,
        .offset = static_cast<int64_t>(leadingGarbage),
        .n_buffers = 1,
        .n_children = 4,
        .buffers = batch->structBuffers.data(),
        .children = batch->arrayChildren.data(),
        .dictionary = nullptr,
        .release =
            +[](ArrowArray* self)
            {
                delete static_cast<ForeignBatch*>(self->private_data);
                self->release = nullptr;
            },
        .private_data = batch};
    outSchema = ArrowSchema{
        .format = "+s",
        .name = "",
        .metadata = nullptr,
        .flags = 0,
        .n_children = 4,
        .children = batch->schemaChildren.data(),
        .dictionary = nullptr,
        .release = +[](ArrowSchema* self) { self->release = nullptr; },
        .private_data = nullptr};
}
}

/// Runs: row buffer -> Scan(row) -> Emit(arrow) -> ArrowTransformPipelineStage -> Scan(arrow) -> Emit(row).
/// The parameter selects between compiled and interpreted execution.
class ArrowTransformPipelineStageTest : public Testing::BaseUnitTest, public ::testing::WithParamInterface<bool>
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ArrowTransformPipelineStageTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        bufferManager = BufferManager::create(
            64UL * 1024 * 1024, 0.5, BufferAlignment{64}, BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());
    }

    Testing::TestSchema schema{
        UnqualifiedUnboundField{Identifier::parse("id"), DataType{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE}},
        UnqualifiedUnboundField{Identifier::parse("value"), DataType{DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE}},
        UnqualifiedUnboundField{Identifier::parse("flag"), DataType{DataType::Type::BOOLEAN, DataType::NULLABLE::NOT_NULLABLE}},
        UnqualifiedUnboundField{Identifier::parse("name"), DataType{DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE}}};

    std::shared_ptr<ArrowBufferRef> arrowLayout()
    {
        return std::dynamic_pointer_cast<ArrowBufferRef>(LowerSchemaProvider::lowerSchemaToArrow(BUFFER_SIZE, schema));
    }

    /// Feeds @param inputBuffers through a compiled pipeline Scan(input) -> Emit(output) and returns what it emitted
    std::vector<TupleBuffer> runCompiled(
        const std::shared_ptr<TupleBufferRef>& inputRef, const std::shared_ptr<TupleBufferRef>& outputRef, const std::vector<TupleBuffer>& inputBuffers)
    {
        ScanPhysicalOperator scan(inputRef, inputRef->getAllFieldNames());
        scan.setChild(PhysicalOperator(EmitPhysicalOperator(OperatorHandlerId(0), outputRef)));
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers{
            {OperatorHandlerId(0), std::make_shared<EmitOperatorHandler>()}};
        nautilus::engine::Options options;
        options.setOption("engine.Compilation", GetParam());
        options.setOption("engine.backend", std::string("mlir"));
        CompiledExecutablePipelineStage stage(std::make_shared<Pipeline>(PhysicalOperator(scan)), handlers, options);

        CollectingContext context{bufferManager};
        stage.start(context);
        for (const auto& buffer : inputBuffers)
        {
            stage.execute(buffer, context);
        }
        stage.stop(context);
        return context.emitted;
    }

    std::vector<Row> runThroughTransform(const std::vector<Row>& input, const ArrowTransformPipelineStage::Transform& transform)
    {
        const auto rowRef = LowerSchemaProvider::lowerSchema(BUFFER_SIZE, schema, MemoryLayoutType::ROW_LAYOUT);

        Testing::TestTupleBuffer testBuffer{schema};
        std::vector<TupleBuffer> inputBuffers{bufferManager->getBufferBlocking()};
        inputBuffers.back().setNumberOfTuples(0);
        auto view = testBuffer.open(inputBuffers.back(), bufferManager.get());
        for (const auto& row : input)
        {
            view.append(row.id, row.value, row.flag, row.name);
        }
        inputBuffers.back().setOriginId(INITIAL<OriginId>);
        inputBuffers.back().setSequenceNumber(INITIAL<SequenceNumber>);
        inputBuffers.back().setChunkNumber(INITIAL<ChunkNumber>);
        inputBuffers.back().setLastChunk(true);

        auto arrowBuffers = runCompiled(rowRef, arrowLayout(), inputBuffers);

        ArrowTransformPipelineStage stage{arrowLayout(), arrowLayout(), transform};
        CollectingContext context{bufferManager};
        for (const auto& buffer : arrowBuffers)
        {
            stage.execute(buffer, context);
        }
        transformedBuffers = context.emitted;

        std::vector<Row> rows;
        for (auto& buffer : runCompiled(arrowLayout(), rowRef, transformedBuffers))
        {
            auto outputView = testBuffer.open(buffer, bufferManager.get());
            for (size_t i = 0; i < outputView.getNumberOfTuples(); ++i)
            {
                rows.push_back(Row{
                    outputView[i]["id"].as<int64_t>(),
                    outputView[i]["value"].as<std::optional<double>>(),
                    outputView[i]["flag"].as<bool>(),
                    outputView[i]["name"].as<std::optional<std::string>>()});
            }
        }
        return rows;
    }

    std::shared_ptr<BufferManager> bufferManager;
    std::vector<TupleBuffer> transformedBuffers;
};

TEST_P(ArrowTransformPipelineStageTest, TransformsAllTypesThroughTheAbi)
{
    const std::vector<Row> input{
        {1, 1.5, true, "one"}, {2, std::nullopt, false, std::nullopt}, {3, 3.5, false, ""}, {4, -4.5, true, "four"}};

    const auto output = runThroughTransform(
        input,
        [](ArrowArray& in, ArrowSchema& inSchema, ArrowArray& out, ArrowSchema& outSchema)
        {
            EXPECT_EQ(std::string_view{inSchema.format}, "+s");
            EXPECT_EQ(std::string_view{inSchema.children[0]->format}, "l");
            EXPECT_EQ(std::string_view{inSchema.children[0]->name}, "ID"); /// Identifiers are canonicalized to upper case
            EXPECT_EQ(std::string_view{inSchema.children[1]->format}, "g");
            EXPECT_EQ(inSchema.children[1]->flags & ARROW_FLAG_NULLABLE, ARROW_FLAG_NULLABLE);
            EXPECT_EQ(std::string_view{inSchema.children[2]->format}, "b");
            EXPECT_EQ(std::string_view{inSchema.children[3]->format}, "u");

            auto rows = readForeign(in);
            for (auto& row : rows)
            {
                row.id *= 10;
                row.flag = !row.flag;
                row.value = row.value.transform([](const double value) { return value * 2; });
                row.name = row.name.transform([](const std::string& name) { return name + "!"; });
            }
            writeForeign(rows, 3, out, outSchema);
        });

    const std::vector<Row> expected{
        {10, 3.0, false, "one!"}, {20, std::nullopt, true, std::nullopt}, {30, 7.0, true, "!"}, {40, -9.0, false, "four!"}};
    EXPECT_EQ(output, expected);
}

TEST_P(ArrowTransformPipelineStageTest, EmptyOutputStillCompletesTheSequence)
{
    const auto output = runThroughTransform(
        {{1, 1.5, true, "one"}}, [](ArrowArray&, ArrowSchema&, ArrowArray& out, ArrowSchema& outSchema) { writeForeign({}, 0, out, outSchema); });
    EXPECT_TRUE(output.empty());
    ASSERT_EQ(transformedBuffers.size(), 1);
    EXPECT_EQ(transformedBuffers[0].getNumberOfTuples(), 0);
    EXPECT_TRUE(transformedBuffers[0].isLastChunk());
}

TEST_P(ArrowTransformPipelineStageTest, GrowingBatchSpansMultipleChunks)
{
    /// Strings larger than a buffer and many rows force multiple output buffers and multiple child buffers per column
    std::vector<Row> grown;
    for (int64_t i = 0; i < 2000; ++i)
    {
        grown.push_back({i, static_cast<double>(i), i % 2 == 0, std::string(i % 500 == 0 ? BUFFER_SIZE * 2 : 50, 'x')});
    }
    const auto output = runThroughTransform(
        {{1, 1.5, true, "one"}},
        [&grown](ArrowArray&, ArrowSchema&, ArrowArray& out, ArrowSchema& outSchema) { writeForeign(grown, 1, out, outSchema); });
    EXPECT_EQ(output, grown);

    ASSERT_GT(transformedBuffers.size(), 1);
    std::vector<ChunkNumber> chunkNumbers;
    for (size_t i = 0; i < transformedBuffers.size(); ++i)
    {
        chunkNumbers.push_back(transformedBuffers[i].getChunkNumber());
        EXPECT_EQ(transformedBuffers[i].isLastChunk(), i + 1 == transformedBuffers.size());
        EXPECT_EQ(transformedBuffers[i].getSequenceNumber(), INITIAL<SequenceNumber>);
    }
    std::ranges::sort(chunkNumbers);
    EXPECT_EQ(std::ranges::adjacent_find(chunkNumbers), chunkNumbers.end());
}

TEST_P(ArrowTransformPipelineStageTest, PayloadSpanningChildBuffersIsContiguousInTheExport)
{
    std::vector<Row> input;
    for (int64_t i = 0; i < 40; ++i)
    {
        input.push_back({i, std::nullopt, false, std::string(BUFFER_SIZE / 8, static_cast<char>('a' + (i % 26)))});
    }
    bool sawMultipleChildBuffers = false;
    const auto output = runThroughTransform(
        input,
        [&](ArrowArray& in, ArrowSchema&, ArrowArray& out, ArrowSchema& outSchema)
        {
            auto rows = readForeign(in);
            sawMultipleChildBuffers = sawMultipleChildBuffers || rows.size() * (BUFFER_SIZE / 8) > BUFFER_SIZE;
            writeForeign(rows, 0, out, outSchema);
        });
    EXPECT_TRUE(sawMultipleChildBuffers);
    EXPECT_EQ(output, input);
}

TEST_P(ArrowTransformPipelineStageTest, RejectsBatchThatViolatesTheSchema)
{
    const auto expectInvalid = [this](const ArrowTransformPipelineStage::Transform& transform)
    { ASSERT_EXCEPTION_ERRORCODE(runThroughTransform({{1, 1.5, true, "one"}}, transform), ErrorCode::InvalidArrowData); };

    /// Other type
    expectInvalid(
        [](ArrowArray&, ArrowSchema&, ArrowArray& out, ArrowSchema& outSchema)
        {
            writeForeign({{1, 1.5, true, "one"}}, 0, out, outSchema);
            outSchema.children[1]->format = "f";
        });
    /// Null in a non-nullable field
    expectInvalid(
        [](ArrowArray&, ArrowSchema&, ArrowArray& out, ArrowSchema& outSchema)
        {
            writeForeign({{1, 1.5, true, "one"}}, 0, out, outSchema);
            static constexpr std::array<uint8_t, 1> ALL_NULL{0};
            auto* id = out.children[0];
            id->buffers[0] = ALL_NULL.data();
            id->null_count = 1;
        });
    /// Missing column
    expectInvalid(
        [](ArrowArray&, ArrowSchema&, ArrowArray& out, ArrowSchema& outSchema)
        {
            writeForeign({{1, 1.5, true, "one"}}, 0, out, outSchema);
            out.n_children = 3;
        });
}

TEST_P(ArrowTransformPipelineStageTest, ReleasesInputAndOutputWhenTransformThrows)
{
    uint32_t referencesDuringTransform = 0;
    auto buffer = bufferManager->getBufferBlocking();
    buffer.setNumberOfTuples(0);
    ArrowTransformPipelineStage stage{
        arrowLayout(),
        arrowLayout(),
        [&](ArrowArray&, ArrowSchema&, ArrowArray&, ArrowSchema&)
        {
            referencesDuringTransform = buffer.getReferenceCounter();
            throw InvalidArrowData("Failed by the test");
        }};
    CollectingContext context{bufferManager};
    ASSERT_EXCEPTION_ERRORCODE(stage.execute(buffer, context), ErrorCode::InvalidArrowData);
    EXPECT_GT(referencesDuringTransform, 1);
    EXPECT_EQ(buffer.getReferenceCounter(), 1);
}

INSTANTIATE_TEST_SUITE_P(, ArrowTransformPipelineStageTest, ::testing::Bool(), [](const auto& info) { return info.param ? "Compiled" : "Interpreted"; });

}
/// NOLINTEND(readability-magic-numbers, cppcoreguidelines-pro-bounds-pointer-arithmetic, cppcoreguidelines-owning-memory)
