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
#include <memory>
#include <mutex>
#include <string>
#include <thread>
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

#include <ArrowFlightSink.hpp>
#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/flight/server.h>

namespace NES
{

/// NOLINTBEGIN(readability-magic-numbers)

/// Minimal Flight server that stores all RecordBatches received via DoPut.
class RecordingFlightServer : public arrow::flight::FlightServerBase
{
public:
    arrow::Status DoPut(
        const arrow::flight::ServerCallContext&,
        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
        std::unique_ptr<arrow::flight::FlightMetadataWriter>) override
    {
        auto batchesResult = reader->ToRecordBatches();
        if (!batchesResult.ok())
        {
            return batchesResult.status();
        }
        std::lock_guard lock(mutex);
        for (auto& batch : *batchesResult)
        {
            batches.push_back(std::move(batch));
        }
        return arrow::Status::OK();
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> getBatches()
    {
        std::lock_guard lock(mutex);
        return batches;
    }

private:
    std::mutex mutex;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
};

/// Minimal PipelineExecutionContext for the sink test.
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

class ArrowFlightSinkTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("ArrowFlightSinkTest.log", LogLevel::LOG_DEBUG);
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();

        server = std::make_unique<RecordingFlightServer>();
        auto location = arrow::flight::Location::Parse("grpc://0.0.0.0:0");
        ASSERT_TRUE(location.ok());
        arrow::flight::FlightServerOptions opts(*location);
        ASSERT_TRUE(server->Init(opts).ok());
        serverEndpoint = "grpc://localhost:" + std::to_string(server->port());

        serverThread = std::jthread([this] { auto st = server->Serve(); });
    }

    void TearDown() override
    {
        auto status = server->Shutdown();
        ASSERT_TRUE(status.ok()) << status.ToString();
        serverThread.join();
        BaseUnitTest::TearDown();
    }

    std::unique_ptr<RecordingFlightServer> server;
    std::jthread serverThread;
    std::string serverEndpoint;
};

/// Writes Arrow-layout tuples through ArrowFlightSink, verifies the Flight server received correct RecordBatches.
TEST_F(ArrowFlightSinkTest, SendRecordBatchesViaFlight)
{
    using Row = std::tuple<int32_t, uint64_t, double>;
    std::vector<Row> input = {{1, 100, 1.5}, {-42, 999, 2.718}, {0, 0, 0.0}};

    constexpr uint64_t bufSize = 8192;
    auto schema = Testing::schemaOf<Row>();
    auto bufMgr = BufferManager::create(bufSize, 200);
    SinkTestContext pec(bufMgr);

    /// Create SinkDescriptor via SinkCatalog
    SinkCatalog catalog;
    std::unordered_map<std::string, std::string> sinkConfig;
    sinkConfig["endpoint"] = serverEndpoint;
    sinkConfig["stream_name"] = "test_stream";
    sinkConfig["output_format"] = "Arrow";
    auto descriptorOpt = catalog.getInlineSink(schema, "ArrowFlight", Host("localhost:8080"), sinkConfig, {});
    ASSERT_TRUE(descriptorOpt.has_value()) << "Failed to create ArrowFlight SinkDescriptor";

    /// Create the sink
    auto [bpc, bpl] = createBackpressureChannel();
    ArrowFlightSink sink(std::move(bpc), *descriptorOpt);
    sink.start(pec);

    /// Create a buffer in Arrow layout and fill it
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

    /// Verify the Flight server received the data
    auto batches = server->getBatches();
    ASSERT_FALSE(batches.empty());

    int64_t totalRows = 0;
    for (const auto& batch : batches)
    {
        ASSERT_TRUE(batch->Validate().ok()) << batch->Validate().ToString();
        ASSERT_EQ(batch->num_columns(), 3);
        totalRows += batch->num_rows();
    }
    EXPECT_EQ(totalRows, static_cast<int64_t>(input.size()));

    /// Verify values
    auto batch = batches[0];
    auto col0 = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    auto col1 = std::static_pointer_cast<arrow::UInt64Array>(batch->column(1));
    auto col2 = std::static_pointer_cast<arrow::DoubleArray>(batch->column(2));

    for (int64_t i = 0; i < batch->num_rows(); ++i)
    {
        EXPECT_EQ(col0->Value(i), std::get<0>(input[i])) << "row=" << i;
        EXPECT_EQ(col1->Value(i), std::get<1>(input[i])) << "row=" << i;
        EXPECT_NEAR(col2->Value(i), std::get<2>(input[i]), 1e-10) << "row=" << i;
    }
}

/// Verifies nullable fields are correctly sent via Flight with proper Arrow validity bitmaps.
TEST_F(ArrowFlightSinkTest, NullableFieldsSentWithValidityBitmaps)
{
    using Row = std::tuple<int32_t, std::optional<int64_t>, std::optional<double>>;
    std::vector<Row> input = {{1, 100, 1.5}, {-42, std::nullopt, 2.718}, {0, 999, std::nullopt}, {7, std::nullopt, std::nullopt}};

    constexpr uint64_t bufSize = 8192;
    auto schema = Testing::schemaOf<Row>();
    auto bufMgr = BufferManager::create(bufSize, 200);
    SinkTestContext pec(bufMgr);

    SinkCatalog catalog;
    std::unordered_map<std::string, std::string> sinkConfig;
    sinkConfig["endpoint"] = serverEndpoint;
    sinkConfig["stream_name"] = "nullable_stream";
    sinkConfig["output_format"] = "Arrow";
    auto descriptorOpt = catalog.getInlineSink(schema, "ArrowFlight", Host("localhost:8080"), sinkConfig, {});
    ASSERT_TRUE(descriptorOpt.has_value());

    auto [bpc, bpl] = createBackpressureChannel();
    ArrowFlightSink sink(std::move(bpc), *descriptorOpt);
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

    auto batches = server->getBatches();
    ASSERT_FALSE(batches.empty());
    auto batch = batches[0];
    ASSERT_TRUE(batch->Validate().ok()) << batch->Validate().ToString();
    ASSERT_EQ(batch->num_rows(), static_cast<int64_t>(input.size()));
    ASSERT_EQ(batch->num_columns(), 3);

    auto col0 = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    auto col1 = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
    auto col2 = std::static_pointer_cast<arrow::DoubleArray>(batch->column(2));

    /// col0 is non-nullable
    EXPECT_FALSE(col0->null_count()) << "col0 should have no nulls";
    for (int64_t i = 0; i < batch->num_rows(); ++i)
    {
        EXPECT_EQ(col0->Value(i), std::get<0>(input[i])) << "row=" << i;
    }

    /// col1 is nullable: rows 0,2 valid, rows 1,3 null
    EXPECT_TRUE(col1->IsValid(0));
    EXPECT_TRUE(col1->IsNull(1));
    EXPECT_TRUE(col1->IsValid(2));
    EXPECT_TRUE(col1->IsNull(3));
    EXPECT_EQ(col1->Value(0), 100);
    EXPECT_EQ(col1->Value(2), 999);

    /// col2 is nullable: rows 0,1 valid, rows 2,3 null
    EXPECT_TRUE(col2->IsValid(0));
    EXPECT_TRUE(col2->IsValid(1));
    EXPECT_TRUE(col2->IsNull(2));
    EXPECT_TRUE(col2->IsNull(3));
    EXPECT_NEAR(col2->Value(0), 1.5, 1e-10);
    EXPECT_NEAR(col2->Value(1), 2.718, 1e-10);
}

/// NOLINTEND(readability-magic-numbers)

}
