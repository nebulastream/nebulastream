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
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Sources/SourceCatalog.hpp>
#include <StoreOperatorHandler.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
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

    SourceDescriptor createBinaryStoreDescriptor(const std::filesystem::path& filePath) const
    {
        SourceCatalog catalog;
        const auto logicalSource = catalog.addLogicalSource("replay", getSchema());
        EXPECT_TRUE(logicalSource.has_value());
        const auto physicalSource = catalog.addPhysicalSource(
            *logicalSource,
            "BinaryStore",
            Host("localhost"),
            {{"file_path", filePath.string()}},
            {{"type", "CSV"}});
        EXPECT_TRUE(physicalSource.has_value());
        return *physicalSource;
    }

    void writeRows(
        const std::filesystem::path& filePath,
        const Replay::BinaryStoreCompressionCodec compression,
        const int32_t compressionLevel = 3) const
    {
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
            .schemaText = schemaText(),
        });

        handler.start(context, 0);
        for (const auto row : rows)
        {
            handler.append(reinterpret_cast<const uint8_t*>(&row), sizeof(row));
        }
        handler.stop(QueryTerminationType::Graceful, context);
    }

    std::vector<TestRow> readRows(const std::filesystem::path& filePath)
    {
        auto bufferManager = BufferManager::create(512, 4);
        BinaryStoreSource source(createBinaryStoreDescriptor(filePath));
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
    const std::vector<TestRow> rows{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}};
};

TEST_F(BinaryStoreSourceTest, ReadsLegacyUncompressedBinaryStoreFiles)
{
    const auto filePath = tempDir / "legacy.store";
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::None);

    EXPECT_EQ(readRows(filePath), rows);
    EXPECT_EQ(BinaryStoreSource::readSchemaFromFile(filePath.string()), getSchema());
}

TEST_F(BinaryStoreSourceTest, ReadsSegmentedZstdBinaryStoreFiles)
{
    const auto filePath = tempDir / "compressed.store";
    writeRows(filePath, Replay::BinaryStoreCompressionCodec::Zstd, -3);

    EXPECT_EQ(readRows(filePath), rows);
    EXPECT_EQ(BinaryStoreSource::readSchemaFromFile(filePath.string()), getSchema());
}
}
}
