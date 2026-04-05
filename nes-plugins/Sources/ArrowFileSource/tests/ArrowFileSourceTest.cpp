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

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BackpressureChannel.hpp>
#include <BaseUnitTest.hpp>
#include <PipelineExecutionContext.hpp>
#include <TestTupleBuffer.hpp>

#include <ArrowFileSink.hpp>
#include <ArrowFileSource.hpp>

namespace NES
{

/// NOLINTBEGIN(readability-magic-numbers)

struct SourceTestPEC final : PipelineExecutionContext
{
    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return true; }
    TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }
    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return INITIAL<WorkerThreadId>; }
    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }
    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }
    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return handlers; }
    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>&) override {}
    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override {}
    TupleBuffer& pinBuffer(TupleBuffer&& tb) override { pinned.emplace_back(std::make_unique<TupleBuffer>(std::move(tb))); return *pinned.back(); }

    std::shared_ptr<BufferManager> bufferManager;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
    std::vector<std::unique_ptr<TupleBuffer>> pinned;

    explicit SourceTestPEC(std::shared_ptr<BufferManager> bm) : bufferManager(std::move(bm)) {}
};

/// Helper: write rows through ArrowFileSink to create an .arrows file
template <typename Row>
void writeArrowFile(
    const std::filesystem::path& path,
    const Schema& schema,
    const std::vector<Row>& rows,
    uint64_t bufSize,
    std::shared_ptr<BufferManager> bufMgr,
    uint64_t rowsPerBatch = 0)
{
    SourceTestPEC pec(bufMgr);
    SinkCatalog catalog;
    std::unordered_map<std::string, std::string> sinkConfig;
    sinkConfig["file_path"] = path.string();
    sinkConfig["output_format"] = "Arrow";
    auto descriptorOpt = catalog.getInlineSink(schema, "ArrowFile", Host("localhost:8080"), sinkConfig, {});
    ASSERT_TRUE(descriptorOpt.has_value()) << "Failed to create ArrowFile SinkDescriptor";

    auto [bpc, bpl] = createBackpressureChannel();
    ArrowFileSink sink(std::move(bpc), *descriptorOpt);
    sink.start(pec);

    auto arrowBufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(bufSize, schema, "Arrow", {});

    if (rowsPerBatch == 0)
    {
        rowsPerBatch = rows.size();
    }

    for (size_t start = 0; start < rows.size(); start += rowsPerBatch)
    {
        auto buffer = bufMgr->getBufferBlocking();
        buffer.setSequenceNumber(SequenceNumber(start + 1));
        buffer.setChunkNumber(ChunkNumber(1));
        buffer.setLastChunk(true);
        buffer.setOriginId(INITIAL<OriginId>);

        Testing::TestTupleBuffer ttb(schema);
        auto view = ttb.openWithBufferRef(buffer, arrowBufRef, bufMgr.get());
        const size_t end = std::min(start + rowsPerBatch, rows.size());
        for (size_t i = start; i < end; ++i)
        {
            view.append(rows[i]);
        }
        sink.execute(buffer, pec);
    }
    sink.stop(pec);
}

/// Helper: create an ArrowFileSource from a file path + schema
std::unique_ptr<ArrowFileSource> createSource(const std::filesystem::path& path, const Schema& schema)
{
    SourceCatalog catalog;
    auto logicalSource = catalog.addLogicalSource("test_src", schema);
    EXPECT_TRUE(logicalSource.has_value());

    std::unordered_map<std::string, std::string> sourceConfig;
    sourceConfig["file_path"] = path.string();
    std::unordered_map<std::string, std::string> parserConfig;
    parserConfig["type"] = "ARROW";

    auto descriptorOpt = catalog.getInlineSource("ArrowFile", schema, Host("localhost:8080"), parserConfig, sourceConfig);
    EXPECT_TRUE(descriptorOpt.has_value());

    return std::make_unique<ArrowFileSource>(*descriptorOpt);
}

class ArrowFileSourceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("ArrowFileSourceTest.log", LogLevel::LOG_DEBUG);
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        auto uniqueName = std::string("arrow_source_test_") + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".arrows";
        testPath = std::filesystem::temp_directory_path() / uniqueName;
    }

    void TearDown() override
    {
        std::filesystem::remove(testPath);
        BaseUnitTest::TearDown();
    }

    std::filesystem::path testPath;
};

/// Write fixed-width types through sink, read back with source, verify values match.
TEST_F(ArrowFileSourceTest, FixedWidthRoundtrip)
{
    using Row = std::tuple<int32_t, uint64_t, double>;
    std::vector<Row> input = {{1, 100, 1.5}, {-42, 999, 2.718}, {0, 0, 0.0}};

    constexpr uint64_t bufSize = 8192;
    auto schema = Testing::schemaOf<Row>();
    auto bufMgr = BufferManager::create(bufSize, 200);

    writeArrowFile(testPath, schema, input, bufSize, bufMgr);

    auto source = createSource(testPath, schema);
    source->open(bufMgr);

    auto buffer = bufMgr->getBufferBlocking();
    std::stop_source stopSrc;
    auto result = source->fillTupleBuffer(buffer, stopSrc.get_token());
    ASSERT_FALSE(result.isEoS());
    buffer.setNumberOfTuples(result.getNumberOfBytes());

    auto arrowBufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(bufSize, schema, "Arrow", {});
    Testing::TestTupleBuffer ttb(schema);
    auto view = ttb.openWithBufferRef(buffer, arrowBufRef, bufMgr.get());

    ASSERT_EQ(view.getNumberOfTuples(), input.size());
    for (size_t i = 0; i < input.size(); ++i)
    {
        EXPECT_EQ(view[i]["f0"].as<int32_t>(), std::get<0>(input[i])) << "row=" << i;
        EXPECT_EQ(view[i]["f1"].as<uint64_t>(), std::get<1>(input[i])) << "row=" << i;
        EXPECT_NEAR(view[i]["f2"].as<double>(), std::get<2>(input[i]), 1e-10) << "row=" << i;
    }

    /// Next call should be EoS
    auto buffer2 = bufMgr->getBufferBlocking();
    auto result2 = source->fillTupleBuffer(buffer2, stopSrc.get_token());
    EXPECT_TRUE(result2.isEoS());

    source->close();
}

/// Write nullable fields and verify null bitmap conversion survives the roundtrip.
TEST_F(ArrowFileSourceTest, NullableRoundtrip)
{
    using Row = std::tuple<int32_t, std::optional<int64_t>, std::optional<double>>;
    std::vector<Row> input = {{1, 100, 1.5}, {-42, std::nullopt, 2.718}, {0, 999, std::nullopt}, {7, std::nullopt, std::nullopt}};

    constexpr uint64_t bufSize = 8192;
    auto schema = Testing::schemaOf<Row>();
    auto bufMgr = BufferManager::create(bufSize, 200);

    writeArrowFile(testPath, schema, input, bufSize, bufMgr);

    auto source = createSource(testPath, schema);
    source->open(bufMgr);

    auto buffer = bufMgr->getBufferBlocking();
    std::stop_source stopSrc;
    auto result = source->fillTupleBuffer(buffer, stopSrc.get_token());
    ASSERT_FALSE(result.isEoS());
    buffer.setNumberOfTuples(result.getNumberOfBytes());

    auto arrowBufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(bufSize, schema, "Arrow", {});
    Testing::TestTupleBuffer ttb(schema);
    auto view = ttb.openWithBufferRef(buffer, arrowBufRef, bufMgr.get());

    ASSERT_EQ(view.getNumberOfTuples(), 4u);

    EXPECT_EQ(view[0]["f0"].as<int32_t>(), 1);
    EXPECT_EQ(view[0]["f1"].as<int64_t>(), 100);
    EXPECT_NEAR(view[0]["f2"].as<double>(), 1.5, 1e-10);

    EXPECT_EQ(view[1]["f0"].as<int32_t>(), -42);
    EXPECT_EQ(view[1]["f1"].get(), Testing::FieldValue(std::monostate{}));
    EXPECT_NEAR(view[1]["f2"].as<double>(), 2.718, 1e-10);

    EXPECT_EQ(view[2]["f0"].as<int32_t>(), 0);
    EXPECT_EQ(view[2]["f1"].as<int64_t>(), 999);
    EXPECT_EQ(view[2]["f2"].get(), Testing::FieldValue(std::monostate{}));

    EXPECT_EQ(view[3]["f0"].as<int32_t>(), 7);
    EXPECT_EQ(view[3]["f1"].get(), Testing::FieldValue(std::monostate{}));
    EXPECT_EQ(view[3]["f2"].get(), Testing::FieldValue(std::monostate{}));

    source->close();
}

/// Write VARSIZED (string) fields and verify child buffer data roundtrips.
TEST_F(ArrowFileSourceTest, VarSizedRoundtrip)
{
    using Row = std::tuple<uint64_t, std::string>;
    std::vector<Row> input = {{1, "hello"}, {2, "world"}, {3, ""}};

    constexpr uint64_t bufSize = 8192;
    auto schema = Testing::schemaOf<Row>();
    auto bufMgr = BufferManager::create(bufSize, 200);

    writeArrowFile(testPath, schema, input, bufSize, bufMgr);

    auto source = createSource(testPath, schema);
    source->open(bufMgr);

    auto buffer = bufMgr->getBufferBlocking();
    std::stop_source stopSrc;
    auto result = source->fillTupleBuffer(buffer, stopSrc.get_token());
    ASSERT_FALSE(result.isEoS());
    buffer.setNumberOfTuples(result.getNumberOfBytes());

    auto arrowBufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(bufSize, schema, "Arrow", {});
    Testing::TestTupleBuffer ttb(schema);
    auto view = ttb.openWithBufferRef(buffer, arrowBufRef, bufMgr.get());

    ASSERT_EQ(view.getNumberOfTuples(), 3u);
    EXPECT_EQ(view[0]["f0"].as<uint64_t>(), 1u);
    EXPECT_EQ(view[0]["f1"].as<std::string>(), "hello");
    EXPECT_EQ(view[1]["f0"].as<uint64_t>(), 2u);
    EXPECT_EQ(view[1]["f1"].as<std::string>(), "world");
    EXPECT_EQ(view[2]["f0"].as<uint64_t>(), 3u);
    EXPECT_EQ(view[2]["f1"].as<std::string>(), "");

    source->close();
}

/// Write 3 separate batches, read all back across fillTupleBuffer calls.
TEST_F(ArrowFileSourceTest, MultipleBatches)
{
    using Row = std::tuple<uint64_t, int32_t>;
    std::vector<Row> input;
    for (uint64_t i = 0; i < 30; ++i)
    {
        input.emplace_back(i, static_cast<int32_t>(i / 10));
    }

    constexpr uint64_t bufSize = 8192;
    auto schema = Testing::schemaOf<Row>();
    auto bufMgr = BufferManager::create(bufSize, 200);

    /// Write as 3 batches of 10 rows each
    writeArrowFile(testPath, schema, input, bufSize, bufMgr, 10);

    auto source = createSource(testPath, schema);
    source->open(bufMgr);

    std::stop_source stopSrc;
    auto arrowBufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(bufSize, schema, "Arrow", {});
    uint64_t totalRows = 0;

    while (true)
    {
        auto buffer = bufMgr->getBufferBlocking();
        auto result = source->fillTupleBuffer(buffer, stopSrc.get_token());
        if (result.isEoS())
        {
            break;
        }
        buffer.setNumberOfTuples(result.getNumberOfBytes());

        Testing::TestTupleBuffer ttb(schema);
        auto view = ttb.openWithBufferRef(buffer, arrowBufRef, bufMgr.get());
        for (uint64_t i = 0; i < view.getNumberOfTuples(); ++i)
        {
            EXPECT_EQ(view[i]["f0"].as<uint64_t>(), totalRows + i) << "row=" << (totalRows + i);
        }
        totalRows += view.getNumberOfTuples();
    }
    EXPECT_EQ(totalRows, 30u);

    source->close();
}

/// Write a batch with more rows than capacity → verify split across TupleBuffers.
TEST_F(ArrowFileSourceTest, LargeBatchSplitAcrossBuffers)
{
    using Row = std::tuple<uint64_t, double>;

    /// Use a small buffer so capacity is limited
    constexpr uint64_t smallBufSize = 256;
    auto schema = Testing::schemaOf<Row>();

    /// Write with a large buffer so all rows fit in one Arrow batch
    constexpr uint64_t largeBufSize = 8192;
    auto largeBufMgr = BufferManager::create(largeBufSize, 200);

    constexpr size_t numRows = 50;
    std::vector<Row> input;
    for (size_t i = 0; i < numRows; ++i)
    {
        input.emplace_back(static_cast<uint64_t>(i), static_cast<double>(i) * 0.5);
    }

    writeArrowFile(testPath, schema, input, largeBufSize, largeBufMgr);

    /// Read with small buffers
    auto smallBufMgr = BufferManager::create(smallBufSize, 200);
    auto source = createSource(testPath, schema);
    source->open(smallBufMgr);

    std::stop_source stopSrc;
    auto arrowBufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(smallBufSize, schema, "Arrow", {});
    uint64_t totalRows = 0;
    int bufferCount = 0;

    while (true)
    {
        auto buffer = smallBufMgr->getBufferBlocking();
        auto result = source->fillTupleBuffer(buffer, stopSrc.get_token());
        if (result.isEoS())
        {
            break;
        }
        buffer.setNumberOfTuples(result.getNumberOfBytes());
        ++bufferCount;

        Testing::TestTupleBuffer ttb(schema);
        auto view = ttb.openWithBufferRef(buffer, arrowBufRef, smallBufMgr.get());
        for (uint64_t i = 0; i < view.getNumberOfTuples(); ++i)
        {
            EXPECT_EQ(view[i]["f0"].as<uint64_t>(), totalRows + i);
            EXPECT_NEAR(view[i]["f1"].as<double>(), static_cast<double>(totalRows + i) * 0.5, 1e-10);
        }
        totalRows += view.getNumberOfTuples();
    }

    EXPECT_EQ(totalRows, numRows);
    EXPECT_GT(bufferCount, 1) << "Expected multiple buffers due to small buffer size";

    source->close();
}

/// ═══════════════════════════════════════════════════════════════════════════
/// Known-limitation tests.  These document bugs / missing features and are
/// expected to FAIL until the corresponding issue is fixed.
/// ═══════════════════════════════════════════════════════════════════════════

/// ---------------------------------------------------------------------------
/// Limitation 1: BOOLEAN columns.
/// Arrow stores booleans as 1-bit packed arrays. The source's fixed-width
/// memcpy path assumes dataTypeSize bytes per record, which is wrong for
/// booleans (Arrow packs 8 booleans into 1 byte).
/// ---------------------------------------------------------------------------
TEST_F(ArrowFileSourceTest, BooleanColumnRoundtrip)
{
    /// Write 100 boolean rows via Arrow C++ API, read through ArrowFileSource,
    /// verify all values including rows beyond byte boundary (index > 7).
    constexpr int64_t numRows = 100;

    auto arrowSchema = arrow::schema({
        arrow::field("id", arrow::uint64(), /*nullable=*/false),
        arrow::field("flag", arrow::boolean(), /*nullable=*/false),
    });

    arrow::UInt64Builder idBuilder;
    arrow::BooleanBuilder flagBuilder;
    for (int64_t i = 0; i < numRows; ++i)
    {
        ASSERT_TRUE(idBuilder.Append(static_cast<uint64_t>(i)).ok());
        ASSERT_TRUE(flagBuilder.Append(i % 2 == 0).ok());
    }
    auto batch = arrow::RecordBatch::Make(arrowSchema, numRows, {*idBuilder.Finish(), *flagBuilder.Finish()});

    auto fileResult = arrow::io::FileOutputStream::Open(testPath.string());
    ASSERT_TRUE(fileResult.ok());
    auto writerResult = arrow::ipc::MakeStreamWriter(*fileResult, arrowSchema);
    ASSERT_TRUE(writerResult.ok());
    ASSERT_TRUE((*writerResult)->WriteRecordBatch(*batch).ok());
    ASSERT_TRUE((*writerResult)->Close().ok());
    ASSERT_TRUE((*fileResult)->Close().ok());

    Schema schema;
    schema.addField("f0", DataType::Type::UINT64);
    schema.addField("f1", DataType::Type::BOOLEAN);

    constexpr uint64_t bufSize = 8192;
    auto bufMgr = BufferManager::create(bufSize, 200);

    auto source = createSource(testPath, schema);
    source->open(bufMgr);

    auto buffer = bufMgr->getBufferBlocking();
    std::stop_source stopSrc;
    auto result = source->fillTupleBuffer(buffer, stopSrc.get_token());
    ASSERT_FALSE(result.isEoS());
    buffer.setNumberOfTuples(result.getNumberOfBytes());

    auto arrowBufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(bufSize, schema, "Arrow", {});
    Testing::TestTupleBuffer ttb(schema);
    auto view = ttb.openWithBufferRef(buffer, arrowBufRef, bufMgr.get());

    ASSERT_EQ(view.getNumberOfTuples(), static_cast<uint64_t>(numRows));
    for (uint64_t i = 0; i < static_cast<uint64_t>(numRows); ++i)
    {
        EXPECT_EQ(view[i]["f0"].as<uint64_t>(), i) << "row=" << i;
        EXPECT_EQ(view[i]["f1"].as<bool>(), i % 2 == 0) << "row=" << i;
    }

    source->close();
}

/// ---------------------------------------------------------------------------
/// Limitation 2: VARSIZED data exceeding a single child buffer.
/// The source puts all string data for one fillTupleBuffer call into a single
/// child buffer.  If total string bytes exceed bufferProvider->getBufferSize(),
/// the unpooled allocation may fail or produce an oversized buffer that
/// violates the child-buffer-per-slot convention used by ArrowBufferRef::readRecord.
/// ---------------------------------------------------------------------------
TEST_F(ArrowFileSourceTest, VarSizedExceedsSingleChildBuffer)
{
    /// The source stores ALL string data for one fillTupleBuffer call in a
    /// single child buffer.  ArrowBufferRef::readRecord walks child buffers
    /// with a stride of numVarSizedColumns, expecting each fillTupleBuffer call
    /// to produce exactly one child buffer per VARSIZED column.  When multiple
    /// VARSIZED columns exist, or when strings are written across multiple
    /// sink batches that get merged into one source buffer, the single-child-
    /// buffer assumption breaks.
    ///
    /// This test uses two VARSIZED columns.  The sink writes each batch with
    /// its own pair of child buffers (one per column).  The source reads and
    /// merges batches into a single buffer, producing one child buffer per
    /// column instead of interleaving them the way the scan expects.

    using Row = std::tuple<uint64_t, std::string, std::string>;
    std::vector<Row> input;
    for (uint64_t i = 0; i < 20; ++i)
    {
        input.emplace_back(i, "col1_" + std::to_string(i), "col2_" + std::to_string(i));
    }

    constexpr uint64_t bufSize = 8192;
    auto schema = Testing::schemaOf<Row>();
    auto bufMgr = BufferManager::create(bufSize, 200);

    /// Write as 4 small batches so the sink produces 4×2 = 8 child buffers
    writeArrowFile(testPath, schema, input, bufSize, bufMgr, 5);

    auto source = createSource(testPath, schema);
    source->open(bufMgr);

    auto buffer = bufMgr->getBufferBlocking();
    std::stop_source stopSrc;
    auto result = source->fillTupleBuffer(buffer, stopSrc.get_token());
    ASSERT_FALSE(result.isEoS());
    buffer.setNumberOfTuples(result.getNumberOfBytes());

    /// Read back and verify both string columns survive
    auto arrowBufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(bufSize, schema, "Arrow", {});
    Testing::TestTupleBuffer ttb(schema);
    auto view = ttb.openWithBufferRef(buffer, arrowBufRef, bufMgr.get());

    for (uint64_t i = 0; i < view.getNumberOfTuples(); ++i)
    {
        EXPECT_EQ(view[i]["f1"].as<std::string>(), "col1_" + std::to_string(i)) << "row=" << i;
        EXPECT_EQ(view[i]["f2"].as<std::string>(), "col2_" + std::to_string(i)) << "row=" << i;
    }

    source->close();
}

/// ---------------------------------------------------------------------------
/// Limitation 6: No schema validation between Arrow file and NES logical source.
/// If the Arrow file has a different schema (wrong types, different column
/// count) from the CREATE LOGICAL SOURCE declaration, the source should fail
/// with a clear error.  Currently it silently reads garbage or crashes.
/// ---------------------------------------------------------------------------
TEST_F(ArrowFileSourceTest, DISABLED_SchemaMismatchDetected)
{
    /// Write an Arrow file with schema {uint64, float64}
    using WriteRow = std::tuple<uint64_t, double>;
    std::vector<WriteRow> input = {{1, 1.5}, {2, 2.5}};
    constexpr uint64_t bufSize = 8192;
    auto writeSchema = Testing::schemaOf<WriteRow>();
    auto bufMgr = BufferManager::create(bufSize, 200);
    writeArrowFile(testPath, writeSchema, input, bufSize, bufMgr);

    /// Declare a different NES schema: {uint64, int32, float64} (3 columns vs 2)
    Schema readSchema;
    readSchema.addField("f0", DataType::Type::UINT64);
    readSchema.addField("f1", DataType::Type::INT32);
    readSchema.addField("f2", DataType::Type::FLOAT64);

    auto source = createSource(testPath, readSchema);

    /// open() or the first fillTupleBuffer should detect the mismatch and throw,
    /// not silently produce corrupt data.
    EXPECT_ANY_THROW(source->open(bufMgr));

    /// If open didn't throw, fillTupleBuffer should.
    if (::testing::Test::HasFailure()) return;
    auto buffer = bufMgr->getBufferBlocking();
    std::stop_source stopSrc;
    EXPECT_ANY_THROW(source->fillTupleBuffer(buffer, stopSrc.get_token()));
}

/// ---------------------------------------------------------------------------
/// Limitation 6b: Schema type mismatch (same column count, wrong types).
/// Arrow file has {uint64, float64} but NES schema says {uint64, int32}.
/// ---------------------------------------------------------------------------
TEST_F(ArrowFileSourceTest, DISABLED_SchemaTypeMismatchDetected)
{
    using WriteRow = std::tuple<uint64_t, double>;
    std::vector<WriteRow> input = {{1, 1.5}, {2, 2.5}};
    constexpr uint64_t bufSize = 8192;
    auto writeSchema = Testing::schemaOf<WriteRow>();
    auto bufMgr = BufferManager::create(bufSize, 200);
    writeArrowFile(testPath, writeSchema, input, bufSize, bufMgr);

    /// Declare NES schema with wrong type for second column: int32 instead of float64
    Schema readSchema;
    readSchema.addField("f0", DataType::Type::UINT64);
    readSchema.addField("f1", DataType::Type::INT32);

    auto source = createSource(testPath, readSchema);
    EXPECT_ANY_THROW(source->open(bufMgr));
}

/// ---------------------------------------------------------------------------
/// Limitation 9: No file-not-found error at registration time.
/// The file path is only validated in open(), not during CREATE PHYSICAL SOURCE.
/// Ideally validateAndFormat should reject a path that doesn't exist.
/// ---------------------------------------------------------------------------
TEST_F(ArrowFileSourceTest, DISABLED_NonexistentFileRejectedAtValidation)
{
    Schema schema;
    schema.addField("f0", DataType::Type::UINT64);

    std::unordered_map<std::string, std::string> config;
    config["file_path"] = "/tmp/this_file_does_not_exist_12345.arrows";

    /// validateAndFormat is called during CREATE PHYSICAL SOURCE.
    /// It should reject a nonexistent file path.
    EXPECT_ANY_THROW(ArrowFileSource::validateAndFormat(config));
}

/// NOLINTEND(readability-magic-numbers)

}
