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

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ios>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configuration/WorkerConfiguration.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <MemoryLayout/RowLayoutField.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/FileUtil.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterTestUtil.hpp>
#include <TestTaskQueue.hpp>

namespace
{
std::vector<size_t> getVarSizedFieldOffsets(const NES::Schema& schema)
{
    size_t priorFieldOffset = 0;
    std::vector<size_t> varSizedFieldOffsets;
    for (const auto& field : schema)
    {
        if (field.dataType.isType(NES::DataType::Type::VARSIZED))
        {
            varSizedFieldOffsets.emplace_back(priorFieldOffset);
        }
        priorFieldOffset += field.dataType.getSizeInBytes();
    }
    return varSizedFieldOffsets;
}
}

/// NOLINTBEGIN(readability-magic-numbers)
namespace NES
{
class SmallFilesTest : public Testing::BaseUnitTest
{
    struct TestFile
    {
        std::filesystem::path fileName;
        std::vector<InputFormatterTestUtil::TestDataTypes> schemaFieldTypes;
    };

    using enum InputFormatterTestUtil::TestDataTypes;
    std::unordered_map<std::string, TestFile> testFileMap{
        {"TwoIntegerColumns", TestFile{.fileName = "TwoIntegerColumns", .schemaFieldTypes = {INT32, INT32}}},
        {"Bimbo", /// https://github.com/cwida/public_bi_benchmark/blob/master/benchmark/Bimbo/
         TestFile{
             .fileName = "Bimbo",
             .schemaFieldTypes = {INT16, INT16, INT32, INT16, FLOAT64, INT32, INT16, INT32, INT16, INT16, FLOAT64, INT16}}},
        {"Food", /// https://github.com/cwida/public_bi_benchmark/blob/master/benchmark/Food/
         TestFile{.fileName = "Food", .schemaFieldTypes = {INT16, INT32, VARSIZED, VARSIZED, INT16, FLOAT64}}},
        {"Spacecraft_Telemetry", /// generated
         TestFile{.fileName = "Spacecraft_Telemetry", .schemaFieldTypes = {INT32, UINT32, BOOLEAN, CHAR, VARSIZED, FLOAT32, FLOAT64}}}};

    SourceCatalog sourceCatalog;

public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("InputFormatterTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup InputFormatterTest test class.");
    }
    void SetUp() override { BaseUnitTest::SetUp(); }

    void TearDown() override { BaseUnitTest::TearDown(); }

    static bool writeBinaryToFile(std::span<const char> data, const std::filesystem::path& filepath, bool append)
    {
        if (const auto parentPath = filepath.parent_path(); !parentPath.empty())
        {
            create_directories(parentPath);
        }

        std::ios_base::openmode openMode = std::ios::binary;
        openMode |= append ? std::ios::app : std::ios::trunc;

        std::ofstream file(filepath, openMode);
        if (not file)
        {
            throw InvalidConfigParameter("Could not open file: {}", filepath.string());
        }

        file.write(data.data(), static_cast<std::streamsize>(data.size_bytes()));

        return true;
    }

    struct TestConfig
    {
        std::string testFileName;
        std::string formatterType;
        bool hasSpanningTuples;
        size_t numberOfIterations;
        size_t numberOfThreads;
        size_t sizeOfRawBuffers;
    };

    struct SetupResult
    {
        NES::Schema schema;
        size_t sizeOfFormattedBuffers;
        size_t numberOfExpectedRawBuffers;
        size_t numberOfRequiredFormattedBuffers;
        std::string currentTestFileName;
        std::string currentTestFilePath;
    };

    size_t
    getNumberOfExpectedBuffers(const TestConfig& testConfig, const std::filesystem::path& testFilePath, size_t sizeOfSchemaInBytes) const
    {
        const auto sizeOfFormattedBuffers = Configurations::WorkerConfiguration().bufferSizeInBytes.getValue();
        PRECONDITION(
            sizeOfFormattedBuffers >= sizeOfSchemaInBytes, "The formatted buffer must be large enough to hold at least one tuple.");

        const auto fileSizeInBytes = std::filesystem::file_size(testFilePath);
        const auto numberOfExpectedRawBuffers
            = (fileSizeInBytes / testConfig.sizeOfRawBuffers) + static_cast<uint64_t>(fileSizeInBytes % testConfig.sizeOfRawBuffers != 0);
        return numberOfExpectedRawBuffers;
    }
    SetupResult setupTest(const TestConfig& testConfig, InputFormatterTestUtil::ThreadSafeVector<NES::Memory::TupleBuffer>& rawBuffers)
    {
        const auto currentTestFile = testFileMap.at(testConfig.testFileName);
        const auto schema = InputFormatterTestUtil::createSchema(currentTestFile.schemaFieldTypes);
        const auto testDirPath = std::filesystem::path(INPUT_FORMATTER_TEST_DATA) / testConfig.formatterType;
        const auto testFilePath = [](TestFile currentTestFile, const std::filesystem::path& testDirPath, std::string_view formatterType)
        {
            if (const auto testFilePath = Util::findFileByName(currentTestFile.fileName, testDirPath))
            {
                return testFilePath.value();
            }
            throw InvalidConfigParameter(
                "Could not find file test file: {}.<file_ending_of_{}>", testDirPath / currentTestFile.fileName, formatterType);
        }(currentTestFile, testDirPath, testConfig.formatterType);

        const auto sizeOfFormattedBuffers = Configurations::WorkerConfiguration().bufferSizeInBytes.getValue();
        const auto numberOfExpectedRawBuffers = getNumberOfExpectedBuffers(testConfig, testFilePath, schema.getSizeOfSchemaInBytes());
        rawBuffers.reserve(numberOfExpectedRawBuffers);

        /// Create file source, start it using the emit function, and wait for the file source to fill the result buffer vector
        const auto numberOfRequiredSourceBuffers = static_cast<uint16_t>(numberOfExpectedRawBuffers + 1);
        std::shared_ptr<Memory::BufferManager> sourceBufferPool
            = Memory::BufferManager::create(testConfig.sizeOfRawBuffers, numberOfRequiredSourceBuffers);

        /// TODO #774: Sources sometimes need an extra buffer (reason currently unknown)
        const auto fileSource = InputFormatterTestUtil::createFileSource(
            sourceCatalog, testFilePath, schema, std::move(sourceBufferPool), numberOfRequiredSourceBuffers);
        fileSource->start(InputFormatterTestUtil::getEmitFunction(rawBuffers));
        rawBuffers.waitForSize(numberOfExpectedRawBuffers);
        INVARIANT(
            fileSource->tryStop(std::chrono::milliseconds(1000)) == Sources::SourceReturnType::TryStopResult::SUCCESS,
            "Failed to stop source.");
        INVARIANT(
            numberOfExpectedRawBuffers == rawBuffers.size(),
            "Expected to have {} raw buffers, but got: {}",
            numberOfExpectedRawBuffers,
            rawBuffers.size());

        /// We assume that we don't need more than two times the number of buffers to represent the formatted data than we need to represent the raw data
        const auto numberOfRequiredFormattedBuffers = static_cast<uint32_t>((rawBuffers.size() + 1) * 2);

        return SetupResult{
            .schema = std::move(schema),
            .sizeOfFormattedBuffers = sizeOfFormattedBuffers,
            .numberOfExpectedRawBuffers = numberOfExpectedRawBuffers,
            .numberOfRequiredFormattedBuffers = numberOfRequiredFormattedBuffers,
            .currentTestFileName = currentTestFile.fileName,
            .currentTestFilePath = testFilePath};
    }

    template <bool WriteExpectedResultsFile>
    bool compareResults(
        const std::vector<std::vector<NES::Memory::TupleBuffer>>& resultBuffers,
        const SetupResult& setupResult,
        const std::vector<size_t>& varSizedFieldOffsets,
        Memory::BufferManager& testBufferManager) const
    {
        /// Combine results and sort them using (ascending on sequence-/chunknumbers)
        auto combinedThreadResults = std::ranges::views::join(resultBuffers);
        std::vector<NES::Memory::TupleBuffer> resultBufferVec(combinedThreadResults.begin(), combinedThreadResults.end());
        std::ranges::sort(
            resultBufferVec.begin(),
            resultBufferVec.end(),
            [](const Memory::TupleBuffer& left, const Memory::TupleBuffer& right)
            {
                if (left.getSequenceNumber() == right.getSequenceNumber())
                {
                    return left.getChunkNumber() < right.getChunkNumber();
                }
                return left.getSequenceNumber() < right.getSequenceNumber();
            });


        /// Load expected results and compare to actual results
        if constexpr (WriteExpectedResultsFile)
        {
            const auto tmpExpectedResultsPath
                = std::filesystem::path(INPUT_FORMATTER_TMP_RESULT_DATA) / std::format("Expected/{}.nes", setupResult.currentTestFileName);
            Util::writeTupleBuffersToFile(resultBufferVec, setupResult.schema, tmpExpectedResultsPath, varSizedFieldOffsets);
        }
        const auto expectedResultsPath
            = std::filesystem::path(INPUT_FORMATTER_TEST_DATA) / std::format("Expected/{}.nes", setupResult.currentTestFileName);
        auto expectedBuffers
            = Util::loadTupleBuffersFromFile(testBufferManager, setupResult.schema, expectedResultsPath, varSizedFieldOffsets);
        return InputFormatterTestUtil::compareTestTupleBuffersOrderSensitive(resultBufferVec, expectedBuffers, setupResult.schema);
    }

    template <bool WriteExpectedResultsFile = false>
    void runTest(const TestConfig& testConfig)
    {
        /// Create vector for result buffers and create emit function to collect buffers from source
        InputFormatterTestUtil::ThreadSafeVector<NES::Memory::TupleBuffer> rawBuffers;

        const auto setupResult = setupTest(testConfig, rawBuffers);

        const auto varSizedFieldOffsets = getVarSizedFieldOffsets(setupResult.schema);
        for (size_t i = 0; i < testConfig.numberOfIterations; ++i)
        {
            /// Prepare TestTaskQueue for processing the input formatter tasks
            auto testBufferManager
                = Memory::BufferManager::create(setupResult.sizeOfFormattedBuffers, setupResult.numberOfRequiredFormattedBuffers);
            auto inputFormatterTask = InputFormatterTestUtil::createInputFormatterTask(setupResult.schema, testConfig.formatterType);
            auto resultBuffers = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(testConfig.numberOfThreads);

            std::vector<TestPipelineTask> pipelineTasks;
            pipelineTasks.reserve(setupResult.numberOfExpectedRawBuffers);
            rawBuffers.modifyBuffer(
                [&](auto& rawBuffersRef)
                {
                    for (size_t bufferIdx = 0; auto& rawBuffer : rawBuffersRef)
                    {
                        const auto currentWorkerThreadId = bufferIdx % testConfig.numberOfThreads;
                        const auto currentSequenceNumber = SequenceNumber(bufferIdx + 1);
                        rawBuffer.setSequenceNumber(currentSequenceNumber);
                        auto pipelineTask = TestPipelineTask(WorkerThreadId(currentWorkerThreadId), rawBuffer, inputFormatterTask);
                        pipelineTasks.emplace_back(std::move(pipelineTask));
                        ++bufferIdx;
                    }
                });

            /// Create test task queue and process input formatter tasks
            auto taskQueue
                = std::make_unique<MultiThreadedTestTaskQueue>(testConfig.numberOfThreads, pipelineTasks, testBufferManager, resultBuffers);
            taskQueue->startProcessing();
            taskQueue->waitForCompletion();

            /// Check results
            const auto isCorrectResult
                = compareResults<WriteExpectedResultsFile>(*resultBuffers, setupResult, varSizedFieldOffsets, *testBufferManager);
            ASSERT_TRUE(isCorrectResult);

            /// Cleanup
            resultBuffers->clear();
            testBufferManager->destroy();
            NES_DEBUG("Destroyed result buffer");
        }
    }
};

TEST_F(SmallFilesTest, testTwoIntegerColumnsJSON)
{
    runTest(TestConfig{
        .testFileName = "TwoIntegerColumns",
        .formatterType = "JSON",
        .hasSpanningTuples = true,
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 4096});
}

TEST_F(SmallFilesTest, testBimboDataJSON)
{
    runTest(TestConfig{
        .testFileName = "Bimbo",
        .formatterType = "JSON",
        .hasSpanningTuples = true,
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 4096});
}

TEST_F(SmallFilesTest, testFoodDataJSON)
{
    runTest(TestConfig{
        .testFileName = "Food",
        .formatterType = "JSON",
        .hasSpanningTuples = true,
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 4096});
}

TEST_F(SmallFilesTest, testSpaceCraftTelemetryJSON)
{
    runTest(TestConfig{
        .testFileName = "Spacecraft_Telemetry",
        .formatterType = "JSON",
        .hasSpanningTuples = true,
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 4096});
}

TEST_F(SmallFilesTest, testTwoIntegerColumns)
{
    runTest(TestConfig{
        .testFileName = "TwoIntegerColumns",
        .formatterType = "CSV",
        .hasSpanningTuples = true,
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 16});
}

TEST_F(SmallFilesTest, testBimboData)
{
    runTest(TestConfig{
        .testFileName = "Bimbo",
        .formatterType = "CSV",
        .hasSpanningTuples = true,
        .numberOfIterations = 10,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 16});
}

TEST_F(SmallFilesTest, testFoodData)
{
    runTest(TestConfig{
        .testFileName = "Food",
        .formatterType = "CSV",
        .hasSpanningTuples = true,
        .numberOfIterations = 1,
        .numberOfThreads = 1,
        .sizeOfRawBuffers = 16});
}

TEST_F(SmallFilesTest, testSpaceCraftTelemetryData)
{
    runTest(
        {.testFileName = "Spacecraft_Telemetry",
         .formatterType = "CSV",
         .hasSpanningTuples = true,
         .numberOfIterations = 1,
         .numberOfThreads = 8,
         .sizeOfRawBuffers = 16});
}


/// Simple test that confirms that we forward already formatted buffers without spanning tuples correctly
TEST_F(SmallFilesTest, testTwoIntegerColumnsNoSpanningBinary)
{
    runTest(TestConfig{
        .testFileName = "TwoIntegerColumns",
        .formatterType = "Native",
        .hasSpanningTuples = false,
        /// Only one iteration possible, because the InputFormatterTask replaces the number of bytes with the number of tuples in a
        /// raw tuple buffer when the tuple buffer is in 'Native' format and has no spanning tuples
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 4096});
}
}
/// NOLINTEND(readability-magic-numbers)
