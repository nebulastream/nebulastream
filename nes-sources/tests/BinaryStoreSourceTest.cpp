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

#include <Sources/BinaryStoreSource.hpp>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <PipelineExecutionContext.hpp>
#include <Replay/BinaryStoreFormat.hpp>
#include <Replay/ReplayStorage.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Sources/SourceCatalog.hpp>
#include <StoreOperatorHandler.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>

namespace NES
{
namespace
{
struct TestRow
{
    uint64_t id;
    uint64_t value;

    bool operator==(const TestRow&) const = default;
};

class BinaryStoreSourceTest : public Testing::BaseUnitTest
{
    struct MockPipelineContext final : PipelineExecutionContext
    {
        explicit MockPipelineContext(std::shared_ptr<BufferManager> bufferManager) : bufferManager(std::move(bufferManager)) { }

        bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return true; }
        void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { }
        TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }
        [[nodiscard]] WorkerThreadId getId() const override { return INITIAL<WorkerThreadId>; }
        [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }
        [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }
        [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return handlers; }
        void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& opHandlers) override { handlers = opHandlers; }

        std::shared_ptr<BufferManager> bufferManager;
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
    };

public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("BinaryStoreSourceTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup BinaryStoreSourceTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        tempDir = std::filesystem::temp_directory_path()
            / ("binary-store-source-test-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(tempDir);
    }

    void TearDown() override
    {
        std::error_code errorCode;
        std::filesystem::remove_all(tempDir, errorCode);
        for (const auto& path : managedRecordingPaths)
        {
            std::filesystem::remove(path, errorCode);
            errorCode.clear();
            std::filesystem::remove(Replay::getRecordingManifestPath(path.string()), errorCode);
            errorCode.clear();
        }
        BaseUnitTest::TearDown();
    }

    [[nodiscard]] static Schema getSchema()
    {
        return Schema{}.addField("id", DataType::Type::UINT64).addField("value", DataType::Type::UINT64);
    }

    [[nodiscard]] static std::string schemaText()
    {
        std::stringstream stream;
        stream << getSchema();
        return stream.str();
    }

    SourceDescriptor createBinaryStoreDescriptorWithConfig(std::unordered_map<std::string, std::string> config) const
    {
        SourceCatalog catalog;
        const auto logicalSource = catalog.addLogicalSource("replay", getSchema());
        EXPECT_TRUE(logicalSource.has_value());
        const auto physicalSource = catalog.addPhysicalSource(
            *logicalSource,
            "BinaryStore",
            Host("localhost"),
            std::move(config),
            {{"type", "CSV"}});
        EXPECT_TRUE(physicalSource.has_value());
        return *physicalSource;
    }

    SourceDescriptor createBinaryStoreDescriptor(const std::filesystem::path& filePath) const
    {
        return createBinaryStoreDescriptorWithConfig({{"file_path", filePath.string()}});
    }

    SourceDescriptor createManagedBinaryStoreDescriptor(
        const std::string& recordingId,
        const std::optional<std::filesystem::path>& filePath = std::nullopt,
        std::unordered_map<std::string, std::string> extraConfig = {}) const
    {
        std::unordered_map<std::string, std::string> config{{"recording_id", recordingId}};
        if (filePath.has_value())
        {
            config.emplace("file_path", filePath->string());
        }
        config.merge(std::move(extraConfig));
        return createBinaryStoreDescriptorWithConfig(std::move(config));
    }

    std::filesystem::path createManagedRecordingPath(const std::string& recordingId)
    {
        const auto path = std::filesystem::path(Replay::getRecordingFilePath(recordingId));
        managedRecordingPaths.push_back(path);
        return path;
    }

    void writeRows(
        const std::filesystem::path& filePath,
        const Replay::BinaryStoreCompressionCodec compression,
        const std::vector<Timestamp::Underlying>& watermarks,
        const int32_t compressionLevel = 3,
        const std::optional<uint64_t> retentionWindowMs = std::nullopt) const
    {
        ASSERT_EQ(watermarks.size(), rows.size());
        auto bufferManager = BufferManager::create(512, 4);
        MockPipelineContext context(bufferManager);
        StoreOperatorHandler handler({
            .filePath = filePath.string(),
            .append = false,
            .header = true,
            .chunkMinBytes = 32,
            .directIO = false,
            .fdatasyncInterval = 0,
            .compression = compression,
            .compressionLevel = compressionLevel,
            .retentionWindowMs = retentionWindowMs,
            .schemaText = schemaText(),
        });

        handler.start(context, 0);
        for (size_t i = 0; i < rows.size(); ++i)
        {
            const auto& row = rows[i];
            handler.append(reinterpret_cast<const uint8_t*>(&row), sizeof(row), Timestamp(watermarks[i]));
        }
        handler.stop(QueryTerminationType::Graceful, context);
    }

    void appendRows(
        const std::filesystem::path& filePath,
        const Replay::BinaryStoreCompressionCodec compression,
        const std::vector<TestRow>& rowsToWrite,
        const std::vector<Timestamp::Underlying>& watermarksToWrite,
        const int32_t compressionLevel = 3,
        const std::optional<uint64_t> retentionWindowMs = std::nullopt) const
    {
        ASSERT_EQ(watermarksToWrite.size(), rowsToWrite.size());
        auto bufferManager = BufferManager::create(512, 4);
        MockPipelineContext context(bufferManager);
        StoreOperatorHandler handler({
            .filePath = filePath.string(),
            .append = true,
            .header = true,
            .chunkMinBytes = 32,
            .directIO = false,
            .fdatasyncInterval = 0,
            .compression = compression,
            .compressionLevel = compressionLevel,
            .retentionWindowMs = retentionWindowMs,
            .schemaText = schemaText(),
        });

        handler.start(context, 0);
        for (size_t i = 0; i < rowsToWrite.size(); ++i)
        {
            handler.append(reinterpret_cast<const uint8_t*>(&rowsToWrite[i]), sizeof(rowsToWrite[i]), Timestamp(watermarksToWrite[i]));
        }
        handler.stop(QueryTerminationType::Graceful, context);
    }

    std::vector<TestRow> readRows(const std::filesystem::path& filePath)
    {
        auto bufferManager = BufferManager::create(512, 4);
        BinaryStoreSource source(createBinaryStoreDescriptor(filePath));
        return readRows(source, bufferManager);
    }

    std::vector<TestRow> readRows(const SourceDescriptor& sourceDescriptor)
    {
        auto bufferManager = BufferManager::create(512, 4);
        BinaryStoreSource source(sourceDescriptor);
        return readRows(source, bufferManager);
    }

    std::vector<TestRow> readRows(BinaryStoreSource& source, const std::shared_ptr<BufferManager>& bufferManager)
    {
        source.open(bufferManager);

        const auto rowLayout = LowerSchemaProvider::lowerSchema(512, getSchema(), MemoryLayoutType::ROW_LAYOUT);
        std::vector<TestRow> result;
        for (;;)
        {
            auto buffer = bufferManager->getBufferBlocking();
            const auto fillResult = source.fillTupleBuffer(buffer, std::stop_token{});
            if (fillResult.isEoS())
            {
                break;
            }

            auto* data = buffer.getAvailableMemoryArea<char>().data();
            for (uint64_t i = 0; i < buffer.getNumberOfTuples(); ++i)
            {
                TestRow row{};
                std::memcpy(&row, data + i * rowLayout->getTupleSize(), sizeof(TestRow));
                result.push_back(row);
            }
        }

        source.close();
        return result;
    }

    std::filesystem::path tempDir;
    std::vector<std::filesystem::path> managedRecordingPaths;
    const std::vector<TestRow> rows{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}};
    const std::vector<Timestamp::Underlying> watermarks{1000, 1000, 2000, 3000, 3000};
};

TEST_F(BinaryStoreSourceTest, ReadsLegacyUncompressedBinaryStoreFiles)
{
    const auto filePath = tempDir / "legacy.store";
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::None, watermarks);

    EXPECT_EQ(readRows(filePath), rows);
    EXPECT_EQ(BinaryStoreSource::readSchemaFromFile(filePath.string()), getSchema());
}

TEST_F(BinaryStoreSourceTest, ReadsSegmentedZstdBinaryStoreFiles)
{
    const auto filePath = tempDir / "compressed.store";
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::Zstd, watermarks, -3);

    EXPECT_EQ(readRows(filePath), rows);
    EXPECT_EQ(BinaryStoreSource::readSchemaFromFile(filePath.string()), getSchema());
}

TEST_F(BinaryStoreSourceTest, TracksPhysicalReplayReadBytesForLegacyFiles)
{
    const auto filePath = tempDir / "legacy-metrics.store";
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::None, watermarks);

    Replay::clearReplayReadBytes();
    EXPECT_EQ(readRows(filePath), rows);
    EXPECT_EQ(Replay::getReplayReadBytes(), rows.size() * sizeof(TestRow));
    Replay::clearReplayReadBytes();
}

TEST_F(BinaryStoreSourceTest, WritesRawStoreManifestWithSegmentWatermarks)
{
    const auto filePath = tempDir / "raw-manifest.store";
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::None, watermarks);

    const auto manifest = Replay::readBinaryStoreManifest(filePath.string());
    ASSERT_EQ(manifest.segments.size(), 3U);

    EXPECT_EQ(manifest.segments[0].logicalSizeBytes, sizeof(TestRow) * 2);
    EXPECT_EQ(manifest.segments[0].getMinWatermark(), Timestamp(1000));
    EXPECT_EQ(manifest.segments[0].getMaxWatermark(), Timestamp(1000));

    EXPECT_EQ(manifest.segments[1].logicalSizeBytes, sizeof(TestRow) * 2);
    EXPECT_EQ(manifest.segments[1].getMinWatermark(), Timestamp(2000));
    EXPECT_EQ(manifest.segments[1].getMaxWatermark(), Timestamp(3000));

    EXPECT_EQ(manifest.segments[2].logicalSizeBytes, sizeof(TestRow));
    EXPECT_EQ(manifest.segments[2].getMinWatermark(), Timestamp(3000));
    EXPECT_EQ(manifest.segments[2].getMaxWatermark(), Timestamp(3000));

    ASSERT_TRUE(manifest.retainedStartWatermark().has_value());
    ASSERT_TRUE(manifest.fillWatermark().has_value());
    EXPECT_EQ(*manifest.retainedStartWatermark(), Timestamp(1000));
    EXPECT_EQ(*manifest.fillWatermark(), Timestamp(3000));
}

TEST_F(BinaryStoreSourceTest, WritesCompressedStoreManifestWithSegmentWatermarks)
{
    const auto filePath = tempDir / "compressed-manifest.store";
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::Zstd, watermarks, -3);

    const auto manifest = Replay::readBinaryStoreManifest(filePath.string());
    ASSERT_EQ(manifest.segments.size(), 3U);

    EXPECT_EQ(manifest.segments[0].logicalSizeBytes, sizeof(TestRow) * 2);
    EXPECT_EQ(manifest.segments[1].logicalSizeBytes, sizeof(TestRow) * 2);
    EXPECT_EQ(manifest.segments[2].logicalSizeBytes, sizeof(TestRow));

    EXPECT_LT(manifest.segments[0].payloadOffset, manifest.segments[1].payloadOffset);
    EXPECT_LT(manifest.segments[1].payloadOffset, manifest.segments[2].payloadOffset);
    EXPECT_EQ(manifest.segments[0].getMinWatermark(), Timestamp(1000));
    EXPECT_EQ(manifest.segments[1].getMaxWatermark(), Timestamp(3000));
    EXPECT_EQ(manifest.segments[2].getMinWatermark(), Timestamp(3000));
    EXPECT_EQ(*manifest.fillWatermark(), Timestamp(3000));
}

TEST_F(BinaryStoreSourceTest, ReplayStorageSelectsSegmentsOverlappingReplayInterval)
{
    const auto filePath = tempDir / "interval-selection.store";
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::None, watermarks);

    const auto selectedSegments = Replay::selectBinaryStoreSegments(
        filePath.string(),
        Replay::BinaryStoreReplaySelection{
            .segmentIds = std::nullopt,
            .scanStartMs = 2000,
            .scanEndMs = 2500});

    EXPECT_THAT(
        selectedSegments | std::views::transform([](const auto& segment) { return segment.segmentId; }) | std::ranges::to<std::vector>(),
        testing::ElementsAre(1));
}

TEST_F(BinaryStoreSourceTest, GarbageCollectsExpiredRawStoreSegmentsByRetentionWindow)
{
    const auto filePath = tempDir / "raw-retention.store";
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::None, watermarks, 3, 1500);

    ASSERT_EQ(readRows(filePath), std::vector<TestRow>({rows[2], rows[3], rows[4]}));

    const auto manifest = Replay::readBinaryStoreManifest(filePath.string());
    ASSERT_EQ(manifest.segments.size(), 2U);
    EXPECT_EQ(manifest.segments.front().payloadOffset, Replay::binaryStoreHeaderSize(static_cast<uint32_t>(schemaText().size())));
    EXPECT_EQ(manifest.segments.front().getMinWatermark(), Timestamp(2000));
    EXPECT_EQ(manifest.segments.back().getMaxWatermark(), Timestamp(3000));
    EXPECT_EQ(*manifest.retainedStartWatermark(), Timestamp(2000));
    EXPECT_EQ(*manifest.fillWatermark(), Timestamp(3000));

    const auto runtimeStatus = Replay::readRecordingRuntimeStatus(filePath.string());
    EXPECT_EQ(runtimeStatus.recordingId, "raw-retention");
    EXPECT_EQ(runtimeStatus.lifecycleState, Replay::RecordingLifecycleState::Filling);
    EXPECT_EQ(runtimeStatus.retentionWindowMs, std::optional<uint64_t>(1500));
    EXPECT_EQ(runtimeStatus.retainedStartWatermark, std::optional<Timestamp>(Timestamp(2000)));
    EXPECT_EQ(runtimeStatus.retainedEndWatermark, std::optional<Timestamp>(Timestamp(3000)));
    EXPECT_EQ(runtimeStatus.fillWatermark, std::optional<Timestamp>(Timestamp(3000)));
    EXPECT_EQ(runtimeStatus.segmentCount, 2U);
    EXPECT_EQ(runtimeStatus.storageBytes, std::filesystem::file_size(filePath));
}

TEST_F(BinaryStoreSourceTest, GarbageCollectsExpiredCompressedStoreSegmentsByRetentionWindow)
{
    const auto filePath = tempDir / "compressed-retention.store";
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::Zstd, watermarks, -3, 1500);

    ASSERT_EQ(readRows(filePath), std::vector<TestRow>({rows[2], rows[3], rows[4]}));

    const auto manifest = Replay::readBinaryStoreManifest(filePath.string());
    ASSERT_EQ(manifest.segments.size(), 2U);
    EXPECT_EQ(manifest.segments.front().payloadOffset, Replay::binaryStoreHeaderSize(static_cast<uint32_t>(schemaText().size())));
    EXPECT_EQ(manifest.segments.front().getMinWatermark(), Timestamp(2000));
    EXPECT_EQ(manifest.segments.back().getMaxWatermark(), Timestamp(3000));
    EXPECT_EQ(*manifest.retainedStartWatermark(), Timestamp(2000));
    EXPECT_EQ(*manifest.fillWatermark(), Timestamp(3000));

    const auto runtimeStatus = Replay::readRecordingRuntimeStatus(filePath.string());
    EXPECT_EQ(runtimeStatus.recordingId, "compressed-retention");
    EXPECT_EQ(runtimeStatus.lifecycleState, Replay::RecordingLifecycleState::Filling);
    EXPECT_EQ(runtimeStatus.retentionWindowMs, std::optional<uint64_t>(1500));
    EXPECT_EQ(runtimeStatus.retainedStartWatermark, std::optional<Timestamp>(Timestamp(2000)));
    EXPECT_EQ(runtimeStatus.retainedEndWatermark, std::optional<Timestamp>(Timestamp(3000)));
    EXPECT_EQ(runtimeStatus.fillWatermark, std::optional<Timestamp>(Timestamp(3000)));
    EXPECT_EQ(runtimeStatus.segmentCount, 2U);
    EXPECT_EQ(runtimeStatus.storageBytes, std::filesystem::file_size(filePath));
}

TEST_F(BinaryStoreSourceTest, DerivesReadyRecordingRuntimeStatusWhenRetentionIsCovered)
{
    const auto filePath = tempDir / "ready-retention.store";
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::None, watermarks, 3, 500);

    const auto runtimeStatus = Replay::readRecordingRuntimeStatus(filePath.string());
    EXPECT_EQ(runtimeStatus.recordingId, "ready-retention");
    EXPECT_EQ(runtimeStatus.lifecycleState, Replay::RecordingLifecycleState::Ready);
    EXPECT_EQ(runtimeStatus.retentionWindowMs, std::optional<uint64_t>(500));
    EXPECT_EQ(runtimeStatus.retainedStartWatermark, std::optional<Timestamp>(Timestamp(2000)));
    EXPECT_EQ(runtimeStatus.fillWatermark, std::optional<Timestamp>(Timestamp(3000)));
}

TEST_F(BinaryStoreSourceTest, PinsManagedRecordingSegmentsWhileReplaySourceIsOpen)
{
    const auto recordingId = std::string("managed-open-")
        + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto filePath = createManagedRecordingPath(recordingId);
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::None, watermarks);

    auto bufferManager = BufferManager::create(512, 4);
    BinaryStoreSource source(createManagedBinaryStoreDescriptor(recordingId));
    source.open(bufferManager);

    auto manifest = Replay::readBinaryStoreManifest(filePath.string());
    ASSERT_FALSE(manifest.segments.empty());
    EXPECT_TRUE(std::ranges::all_of(manifest.segments, [](const auto& segment) { return segment.pinCount == 1; }));

    source.close();
    manifest = Replay::readBinaryStoreManifest(filePath.string());
    EXPECT_TRUE(std::ranges::all_of(manifest.segments, [](const auto& segment) { return segment.pinCount == 0; }));
}

TEST_F(BinaryStoreSourceTest, PinsOnlySelectedManagedRecordingSegmentsWhileReplaySourceIsOpen)
{
    const auto recordingId = std::string("managed-selected-open-")
        + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto filePath = createManagedRecordingPath(recordingId);
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::None, watermarks);

    auto bufferManager = BufferManager::create(512, 4);
    BinaryStoreSource source(createManagedBinaryStoreDescriptor(recordingId, std::nullopt, {{"segment_ids", "1,2"}}));
    source.open(bufferManager);

    auto manifest = Replay::readBinaryStoreManifest(filePath.string());
    ASSERT_EQ(manifest.segments.size(), 3U);
    EXPECT_EQ(manifest.segments[0].pinCount, 0U);
    EXPECT_EQ(manifest.segments[1].pinCount, 1U);
    EXPECT_EQ(manifest.segments[2].pinCount, 1U);

    source.close();
    manifest = Replay::readBinaryStoreManifest(filePath.string());
    EXPECT_TRUE(std::ranges::all_of(manifest.segments, [](const auto& segment) { return segment.pinCount == 0; }));
}

TEST_F(BinaryStoreSourceTest, ReadsOnlySelectedRawReplaySegmentsAndTracksReplayBytes)
{
    const auto recordingId = std::string("managed-raw-selection-")
        + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto filePath = createManagedRecordingPath(recordingId);
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::None, watermarks);

    Replay::clearReplayReadBytes();
    EXPECT_EQ(
        readRows(createManagedBinaryStoreDescriptor(recordingId, std::nullopt, {{"segment_ids", "0,2"}})),
        std::vector<TestRow>({rows[0], rows[1], rows[4]}));
    EXPECT_EQ(Replay::getReplayReadBytes(), 3 * sizeof(TestRow));
    Replay::clearReplayReadBytes();
}

TEST_F(BinaryStoreSourceTest, ReadsOnlySegmentsOverlappingReplayIntervalInCompressedStore)
{
    const auto recordingId = std::string("managed-compressed-interval-")
        + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto filePath = createManagedRecordingPath(recordingId);
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::Zstd, watermarks, -3);

    EXPECT_EQ(
        readRows(createManagedBinaryStoreDescriptor(
            recordingId,
            std::nullopt,
            {{"scan_start_ms", "2000"}, {"scan_end_ms", "2500"}, {"replay_mode", "interval"}})),
        std::vector<TestRow>({rows[2], rows[3]}));
}

TEST_F(BinaryStoreSourceTest, KeepsPinnedExpiredSegmentsDuringRollingRetentionGc)
{
    const auto recordingId = std::string("managed-retention-")
        + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto filePath = createManagedRecordingPath(recordingId);
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::None, watermarks);

    auto bufferManager = BufferManager::create(512, 4);
    BinaryStoreSource source(createManagedBinaryStoreDescriptor(recordingId));
    source.open(bufferManager);

    appendRows(
        filePath,
        Replay::BinaryStoreCompressionCodec::None,
        std::vector<TestRow>{{6, 60}},
        std::vector<Timestamp::Underlying>{4000},
        3,
        1500);

    auto manifest = Replay::readBinaryStoreManifest(filePath.string());
    EXPECT_THAT(
        manifest.segments | std::views::transform([](const auto& segment) { return segment.segmentId; }) | std::ranges::to<std::vector>(),
        testing::Contains(0));
    EXPECT_EQ(manifest.segments.front().pinCount, 1U);

    source.close();

    appendRows(
        filePath,
        Replay::BinaryStoreCompressionCodec::None,
        std::vector<TestRow>{{7, 70}},
        std::vector<Timestamp::Underlying>{4000},
        3,
        1500);

    manifest = Replay::readBinaryStoreManifest(filePath.string());
    EXPECT_THAT(
        manifest.segments | std::views::transform([](const auto& segment) { return segment.segmentId; }) | std::ranges::to<std::vector>(),
        testing::Not(testing::Contains(0)));
}
}
}
