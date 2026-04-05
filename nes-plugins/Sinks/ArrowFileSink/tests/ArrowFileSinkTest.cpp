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
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BackpressureChannel.hpp>
#include <BaseUnitTest.hpp>
#include <PipelineExecutionContext.hpp>
#include <TestTupleBuffer.hpp>

#include <ArrowFileSink.hpp>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

namespace NES
{

/// NOLINTBEGIN(readability-magic-numbers)

struct SinkTestContext final : PipelineExecutionContext
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

    explicit SinkTestContext(std::shared_ptr<BufferManager> bm) : bufferManager(std::move(bm)) {}
};

class ArrowFileSinkTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("ArrowFileSinkTest.log", LogLevel::LOG_DEBUG);
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        auto uniqueName = std::string("arrow_sink_test_") + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".arrows";
        outputPath = std::filesystem::temp_directory_path() / uniqueName;
    }

    void TearDown() override
    {
        std::filesystem::remove(outputPath);
        BaseUnitTest::TearDown();
    }

    std::filesystem::path outputPath;
};

/// Writes fixed-width tuples through ArrowFileSink, reads back with PyArrow-compatible reader.
TEST_F(ArrowFileSinkTest, WriteFixedWidthTypes)
{
    using Row = std::tuple<int32_t, uint64_t, double>;
    std::vector<Row> input = {{1, 100, 1.5}, {-42, 999, 2.718}, {0, 0, 0.0}};

    constexpr uint64_t bufSize = 8192;
    auto schema = Testing::schemaOf<Row>();
    auto bufMgr = BufferManager::create(bufSize, 200);
    SinkTestContext pec(bufMgr);

    SinkCatalog catalog;
    std::unordered_map<std::string, std::string> sinkConfig;
    sinkConfig["file_path"] = outputPath.string();
    sinkConfig["output_format"] = "Arrow";
    auto descriptorOpt = catalog.getInlineSink(schema, "ArrowFile", Host("localhost:8080"), sinkConfig, {});
    ASSERT_TRUE(descriptorOpt.has_value()) << "Failed to create ArrowFile SinkDescriptor";

    auto [bpc, bpl] = createBackpressureChannel();
    ArrowFileSink sink(std::move(bpc), *descriptorOpt);
    sink.start(pec);

    auto arrowBufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(bufSize, schema, "Arrow", {});
    auto buffer = bufMgr->getBufferBlocking();
    buffer.setSequenceNumber(SequenceNumber(1));
    buffer.setChunkNumber(ChunkNumber(1));
    buffer.setLastChunk(true);
    buffer.setOriginId(INITIAL<OriginId>);

    Testing::TestTupleBuffer ttb(schema);
    auto view = ttb.openWithBufferRef(buffer, arrowBufRef, bufMgr.get());
    for (const auto& row : input)
    {
        view.append(row);
    }

    sink.execute(buffer, pec);
    sink.stop(pec);

    /// Read back with Arrow IPC reader
    auto fileResult = arrow::io::ReadableFile::Open(outputPath.string());
    ASSERT_TRUE(fileResult.ok()) << fileResult.status().ToString();
    auto readerResult = arrow::ipc::RecordBatchStreamReader::Open(*fileResult);
    ASSERT_TRUE(readerResult.ok()) << readerResult.status().ToString();
    auto reader = *readerResult;

    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_TRUE(reader->ReadNext(&batch).ok());
    ASSERT_NE(batch, nullptr);
    ASSERT_TRUE(batch->Validate().ok()) << batch->Validate().ToString();
    ASSERT_EQ(batch->num_rows(), static_cast<int64_t>(input.size()));
    ASSERT_EQ(batch->num_columns(), 3);

    auto col0 = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    auto col1 = std::static_pointer_cast<arrow::UInt64Array>(batch->column(1));
    auto col2 = std::static_pointer_cast<arrow::DoubleArray>(batch->column(2));
    for (int64_t i = 0; i < batch->num_rows(); ++i)
    {
        EXPECT_EQ(col0->Value(i), std::get<0>(input[i])) << "row=" << i;
        EXPECT_EQ(col1->Value(i), std::get<1>(input[i])) << "row=" << i;
        EXPECT_NEAR(col2->Value(i), std::get<2>(input[i]), 1e-10) << "row=" << i;
    }

    /// No more batches
    ASSERT_TRUE(reader->ReadNext(&batch).ok());
    EXPECT_EQ(batch, nullptr);
}

/// Writes nullable fields and verifies null bitmaps survive the Arrow IPC roundtrip.
TEST_F(ArrowFileSinkTest, NullableFieldsRoundtrip)
{
    using Row = std::tuple<int32_t, std::optional<int64_t>, std::optional<double>>;
    std::vector<Row> input = {{1, 100, 1.5}, {-42, std::nullopt, 2.718}, {0, 999, std::nullopt}, {7, std::nullopt, std::nullopt}};

    constexpr uint64_t bufSize = 8192;
    auto schema = Testing::schemaOf<Row>();
    auto bufMgr = BufferManager::create(bufSize, 200);
    SinkTestContext pec(bufMgr);

    SinkCatalog catalog;
    std::unordered_map<std::string, std::string> sinkConfig;
    sinkConfig["file_path"] = outputPath.string();
    sinkConfig["output_format"] = "Arrow";
    auto descriptorOpt = catalog.getInlineSink(schema, "ArrowFile", Host("localhost:8080"), sinkConfig, {});
    ASSERT_TRUE(descriptorOpt.has_value());

    auto [bpc, bpl] = createBackpressureChannel();
    ArrowFileSink sink(std::move(bpc), *descriptorOpt);
    sink.start(pec);

    auto arrowBufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(bufSize, schema, "Arrow", {});
    auto buffer = bufMgr->getBufferBlocking();
    buffer.setSequenceNumber(SequenceNumber(1));
    buffer.setChunkNumber(ChunkNumber(1));
    buffer.setLastChunk(true);
    buffer.setOriginId(INITIAL<OriginId>);

    Testing::TestTupleBuffer ttb(schema);
    auto view = ttb.openWithBufferRef(buffer, arrowBufRef, bufMgr.get());
    for (const auto& row : input)
    {
        view.append(row);
    }

    sink.execute(buffer, pec);
    sink.stop(pec);

    auto fileResult = arrow::io::ReadableFile::Open(outputPath.string());
    ASSERT_TRUE(fileResult.ok());
    auto readerResult = arrow::ipc::RecordBatchStreamReader::Open(*fileResult);
    ASSERT_TRUE(readerResult.ok());
    auto reader = *readerResult;

    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_TRUE(reader->ReadNext(&batch).ok());
    ASSERT_NE(batch, nullptr);
    ASSERT_TRUE(batch->Validate().ok()) << batch->Validate().ToString();
    ASSERT_EQ(batch->num_rows(), 4);

    auto col1 = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
    auto col2 = std::static_pointer_cast<arrow::DoubleArray>(batch->column(2));

    EXPECT_TRUE(col1->IsValid(0));
    EXPECT_EQ(col1->Value(0), 100);
    EXPECT_TRUE(col1->IsNull(1));
    EXPECT_TRUE(col1->IsValid(2));
    EXPECT_EQ(col1->Value(2), 999);
    EXPECT_TRUE(col1->IsNull(3));

    EXPECT_TRUE(col2->IsValid(0));
    EXPECT_NEAR(col2->Value(0), 1.5, 1e-10);
    EXPECT_TRUE(col2->IsValid(1));
    EXPECT_NEAR(col2->Value(1), 2.718, 1e-10);
    EXPECT_TRUE(col2->IsNull(2));
    EXPECT_TRUE(col2->IsNull(3));
}

/// Writes multiple batches and verifies all are readable from the IPC stream.
TEST_F(ArrowFileSinkTest, MultipleBatches)
{
    using Row = std::tuple<uint64_t, int32_t>;

    constexpr uint64_t bufSize = 8192;
    auto schema = Testing::schemaOf<Row>();
    auto bufMgr = BufferManager::create(bufSize, 200);
    SinkTestContext pec(bufMgr);

    SinkCatalog catalog;
    std::unordered_map<std::string, std::string> sinkConfig;
    sinkConfig["file_path"] = outputPath.string();
    sinkConfig["output_format"] = "Arrow";
    auto descriptorOpt = catalog.getInlineSink(schema, "ArrowFile", Host("localhost:8080"), sinkConfig, {});
    ASSERT_TRUE(descriptorOpt.has_value());

    auto [bpc, bpl] = createBackpressureChannel();
    ArrowFileSink sink(std::move(bpc), *descriptorOpt);
    sink.start(pec);

    auto arrowBufRef = LowerSchemaProvider::lowerSchemaWithOutputFormat(bufSize, schema, "Arrow", {});

    /// Write 3 separate batches
    for (int batch = 0; batch < 3; ++batch)
    {
        auto buffer = bufMgr->getBufferBlocking();
        buffer.setSequenceNumber(SequenceNumber(batch + 1));
        buffer.setChunkNumber(ChunkNumber(1));
        buffer.setLastChunk(true);
        buffer.setOriginId(INITIAL<OriginId>);

        Testing::TestTupleBuffer ttb(schema);
        auto view = ttb.openWithBufferRef(buffer, arrowBufRef, bufMgr.get());
        for (uint64_t i = 0; i < 10; ++i)
        {
            const Row row{static_cast<uint64_t>(batch * 10 + i), static_cast<int32_t>(batch)};
            view.append(row);
        }
        sink.execute(buffer, pec);
    }
    sink.stop(pec);

    /// Read back and verify all 3 batches
    auto fileResult = arrow::io::ReadableFile::Open(outputPath.string());
    ASSERT_TRUE(fileResult.ok());
    auto readerResult = arrow::ipc::RecordBatchStreamReader::Open(*fileResult);
    ASSERT_TRUE(readerResult.ok());
    auto reader = *readerResult;

    int64_t totalRows = 0;
    int batchCount = 0;
    std::shared_ptr<arrow::RecordBatch> batch;
    while (reader->ReadNext(&batch).ok() && batch != nullptr)
    {
        ASSERT_TRUE(batch->Validate().ok()) << batch->Validate().ToString();
        auto col0 = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));
        for (int64_t i = 0; i < batch->num_rows(); ++i)
        {
            EXPECT_EQ(col0->Value(i), static_cast<uint64_t>(totalRows + i));
        }
        totalRows += batch->num_rows();
        ++batchCount;
    }
    EXPECT_EQ(batchCount, 3);
    EXPECT_EQ(totalRows, 30);
}

/// NOLINTEND(readability-magic-numbers)

}
