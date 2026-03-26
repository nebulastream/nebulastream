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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <BenchmarkSource.hpp>

namespace NES
{

static constexpr size_t TEST_BUFFER_SIZE = 4096;
static constexpr size_t TEST_NUM_BUFFERS = 16;

class BenchmarkSourceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("BenchmarkSourceTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        bufferManager = BufferManager::create(TEST_BUFFER_SIZE, TEST_NUM_BUFFERS);

        /// Create a test file with known content
        testFilePath = std::filesystem::temp_directory_path() / "nes_benchmark_source_test.dat";
        std::ofstream testFile(testFilePath, std::ios::binary);
        testData.resize(TEST_BUFFER_SIZE * 3 + 100); /// slightly more than 3 full buffers
        for (size_t i = 0; i < testData.size(); ++i)
        {
            testData[i] = static_cast<char>(i % 256);
        }
        testFile.write(testData.data(), static_cast<std::streamsize>(testData.size()));
        testFile.close();
    }

    void TearDown() override
    {
        bufferManager->destroy();
        std::filesystem::remove(testFilePath);
        BaseUnitTest::TearDown();
    }

    SourceDescriptor createDescriptor(const std::string& mode, bool loopEnabled = false, const std::string& tmpfsPathOverride = "")
    {
        auto sourceCatalog = SourceCatalog{};
        auto schema = Schema{};
        schema.addField("data", DataTypeProvider::provideDataType(DataType::Type::UINT8));

        const auto logicalSource = sourceCatalog.addLogicalSource("benchSource", schema);
        EXPECT_TRUE(logicalSource.has_value());

        std::unordered_map<std::string, std::string> sourceConfig{
            {"file_path", testFilePath.string()},
            {"mode", mode},
            {"loop", loopEnabled ? "true" : "false"},
        };
        if (!tmpfsPathOverride.empty())
        {
            sourceConfig["tmpfs_path"] = tmpfsPathOverride;
        }

        const auto descriptorResult
            = sourceCatalog.addPhysicalSource(*logicalSource, "Benchmark", std::move(sourceConfig), {{"type", "CSV"}});
        EXPECT_TRUE(descriptorResult.has_value());
        return descriptorResult.value();
    }

    /// Reads all data from the source until EoS and returns the concatenated bytes.
    std::string readAllFromSource(BenchmarkSource& source)
    {
        std::string result;
        std::stop_source stopSource;
        while (true)
        {
            auto buffer = bufferManager->getBufferBlocking();
            auto fillResult = source.fillTupleBuffer(buffer, stopSource.get_token());
            if (fillResult.isEoS())
            {
                break;
            }
            auto area = buffer.getAvailableMemoryArea<char>();
            result.append(area.data(), fillResult.getNumberOfBytes());
        }
        return result;
    }

    std::shared_ptr<BufferManager> bufferManager;
    std::filesystem::path testFilePath;
    std::vector<char> testData;
};

TEST_F(BenchmarkSourceTest, FileModeReadsCorrectData)
{
    auto descriptor = createDescriptor("file");
    auto source = BenchmarkSource(descriptor);
    source.open(bufferManager);

    auto result = readAllFromSource(source);
    source.close();

    ASSERT_EQ(result.size(), testData.size());
    ASSERT_EQ(result, std::string(testData.data(), testData.size()));
}

TEST_F(BenchmarkSourceTest, MemoryModeReadsCorrectData)
{
    auto descriptor = createDescriptor("memory");
    auto source = BenchmarkSource(descriptor);
    source.open(bufferManager);

    auto result = readAllFromSource(source);
    source.close();

    ASSERT_EQ(result.size(), testData.size());
    ASSERT_EQ(result, std::string(testData.data(), testData.size()));
}

TEST_F(BenchmarkSourceTest, InPlaceModeReadsCorrectData)
{
    auto descriptor = createDescriptor("in_place");
    auto source = BenchmarkSource(descriptor);
    source.open(bufferManager);

    auto result = readAllFromSource(source);
    source.close();

    ASSERT_EQ(result.size(), testData.size());
    ASSERT_EQ(result, std::string(testData.data(), testData.size()));
}

TEST_F(BenchmarkSourceTest, InPlaceModeIsZeroCopy)
{
    auto descriptor = createDescriptor("in_place");
    auto source = BenchmarkSource(descriptor);
    source.open(bufferManager);

    /// Get a pooled buffer and record its data pointer
    auto pooledBuffer = bufferManager->getBufferBlocking();
    auto* pooledPtr = pooledBuffer.getAvailableMemoryArea<char>().data();

    std::stop_source stopSource;
    auto fillResult = source.fillTupleBuffer(pooledBuffer, stopSource.get_token());
    ASSERT_FALSE(fillResult.isEoS());

    /// After fill, the buffer should point to different memory (the pre-loaded file data, not the pool)
    auto* afterPtr = pooledBuffer.getAvailableMemoryArea<char>().data();
    ASSERT_NE(afterPtr, pooledPtr);

    source.close();
}

TEST_F(BenchmarkSourceTest, FileLoopModeRewinds)
{
    auto descriptor = createDescriptor("file", /*loopEnabled=*/true);
    auto source = BenchmarkSource(descriptor);
    source.open(bufferManager);

    /// Read all data once
    size_t firstPassBytes = 0;
    std::stop_source stopSource;
    while (true)
    {
        auto buffer = bufferManager->getBufferBlocking();
        auto fillResult = source.fillTupleBuffer(buffer, stopSource.get_token());
        if (fillResult.isEoS())
        {
            break;
        }
        firstPassBytes += fillResult.getNumberOfBytes();
        if (firstPassBytes >= testData.size() * 2)
        {
            break; /// read at least 2 passes worth to confirm looping
        }
    }
    source.close();

    ASSERT_GE(firstPassBytes, testData.size() * 2);
}

TEST_F(BenchmarkSourceTest, MemoryLoopModeRewinds)
{
    auto descriptor = createDescriptor("memory", /*loopEnabled=*/true);
    auto source = BenchmarkSource(descriptor);
    source.open(bufferManager);

    size_t totalBytes = 0;
    std::stop_source stopSource;
    while (totalBytes < testData.size() * 2)
    {
        auto buffer = bufferManager->getBufferBlocking();
        auto fillResult = source.fillTupleBuffer(buffer, stopSource.get_token());
        ASSERT_FALSE(fillResult.isEoS());
        totalBytes += fillResult.getNumberOfBytes();
    }
    source.close();

    ASSERT_GE(totalBytes, testData.size() * 2);
}

#ifdef __linux__
TEST_F(BenchmarkSourceTest, TmpfsColdModeReadsCorrectData)
{
    auto descriptor = createDescriptor("tmpfs_cold");
    auto source = BenchmarkSource(descriptor);
    source.open(bufferManager);

    auto result = readAllFromSource(source);
    source.close();

    ASSERT_EQ(result.size(), testData.size());
    ASSERT_EQ(result, std::string(testData.data(), testData.size()));
}

TEST_F(BenchmarkSourceTest, TmpfsWarmModeReadsCorrectData)
{
    auto descriptor = createDescriptor("tmpfs_warm");
    auto source = BenchmarkSource(descriptor);
    source.open(bufferManager);

    auto result = readAllFromSource(source);
    source.close();

    ASSERT_EQ(result.size(), testData.size());
    ASSERT_EQ(result, std::string(testData.data(), testData.size()));
}

TEST_F(BenchmarkSourceTest, TmpfsCopyIsCleanedUpAfterClose)
{
    auto descriptor = createDescriptor("tmpfs_cold");
    auto source = BenchmarkSource(descriptor);
    source.open(bufferManager);

    /// Find the tmpfs copy
    bool foundCopy = false;
    for (const auto& entry : std::filesystem::directory_iterator("/dev/shm"))
    {
        if (entry.path().filename().string().starts_with("nes_benchmark_"))
        {
            foundCopy = true;
            break;
        }
    }
    ASSERT_TRUE(foundCopy);

    readAllFromSource(source);
    source.close();

    /// After close, the tmpfs copy should be removed
    bool foundAfterClose = false;
    for (const auto& entry : std::filesystem::directory_iterator("/dev/shm"))
    {
        if (entry.path().filename().string().starts_with("nes_benchmark_"))
        {
            foundAfterClose = true;
            break;
        }
    }
    ASSERT_FALSE(foundAfterClose);
}
#endif

}
