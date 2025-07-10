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

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <MemoryLayout/RowLayoutField.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterTestUtil.hpp>
#include <TestTaskQueue.hpp>

/// NOLINTBEGIN(readability-magic-numbers)
namespace NES
{
class SmallFilesTest : public Testing::BaseUnitTest
{
    struct TestFile
    {
        std::string fileName;
        std::vector<InputFormatterTestUtil::TestDataTypes> schemaFieldTypes;
    };

    using enum InputFormatterTestUtil::TestDataTypes;
    std::unordered_map<std::string, TestFile> testFileMap{
        // UINT64 user_id UINT64 page_id UINT64 campaign_id UINT64 ad_type UINT64 event_type UINT64 current_ms UINT64 ip UINT64 d1 UINT64 d2 UINT32 d3 UINT16 d4 FILE
        {"YSB10K", TestFile{.fileName = "ysb_10k_data.csv", .schemaFieldTypes = {UINT64, UINT64, UINT64, UINT64, UINT64, UINT64, UINT64, UINT64, UINT64, UINT32, UINT16}}},
        {"TwoIntegerColumns", TestFile{.fileName = "TwoIntegerColumns_20_Lines.csv", .schemaFieldTypes = {INT32, INT32}}},
        {"Bimbo_1_1000", /// https://github.com/cwida/public_bi_benchmark/blob/master/benchmark/Bimbo/
         TestFile{
             .fileName = "Bimbo_1_1000_Lines.csv",
             .schemaFieldTypes = {INT16, INT16, INT32, INT16, FLOAT64, INT32, INT16, INT32, INT16, INT16, FLOAT64, INT16}}},
        {"Food_1", /// https://github.com/cwida/public_bi_benchmark/blob/master/benchmark/Food/
         TestFile{.fileName = "Food_1_1000_Lines.csv", .schemaFieldTypes = {INT16, INT32, VARSIZED, VARSIZED, INT16, FLOAT64}}},
        {"Spacecraft_Telemetry", /// generated
         TestFile{
             .fileName = "Spacecraft_Telemetry_1000_Lines.csv",
             .schemaFieldTypes = {INT32, UINT32, BOOLEAN, CHAR, VARSIZED, FLOAT32, FLOAT64}}},
        {"TwoIntegerColumns_binary", TestFile{.fileName = "TwoIntegerColumns_20_Lines_binary.bin", .schemaFieldTypes = {INT32, INT32}}},
        {"Bimbo_1_1000_binary",
         TestFile{
             .fileName = "Bimbo_1_1000_Lines_binary.bin",
             .schemaFieldTypes = {INT16, INT16, INT32, INT16, FLOAT64, INT32, INT16, INT32, INT16, INT16, FLOAT64, INT16}}},
        {"Food_1_binary",
         TestFile{.fileName = "Food_1_1000_Lines_binary.bin", .schemaFieldTypes = {INT16, INT32, VARSIZED, VARSIZED, INT16, FLOAT64}}},
        {"Spacecraft_Telemetry_binary", /// generated
         TestFile{
             .fileName = "Spacecraft_Telemetry_1000_Lines_binary.bin",
             .schemaFieldTypes = {INT32, UINT32, BOOLEAN, CHAR, VARSIZED, FLOAT32, FLOAT64}}}};
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
        size_t sizeOfFormattedBuffers;
        std::string fieldDelimiter = "|";
        bool validate = true;
    };

    struct SetupResult
    {
        NES::Schema schema;
        size_t numberOfRequiredFormattedBuffers;
        std::string currentTestFileName;
        std::string currentTestFilePath;
    };

    size_t getNumberOfExpectedBuffers(const TestConfig& testConfig) const
    {
        const auto currentTestFile = testFileMap.at(testConfig.testFileName);
        const auto schema = InputFormatterTestUtil::createSchema(currentTestFile.schemaFieldTypes);
        PRECONDITION(
            testConfig.sizeOfFormattedBuffers >= schema.getSizeOfSchemaInBytes(),
            "The formatted buffer must be large enough to hold at least one tuple.");

        const auto testFilePath = std::filesystem::path(INPUT_FORMATTER_TEST_DATA) / testConfig.formatterType / currentTestFile.fileName;
        const auto fileSizeInBytes = std::filesystem::file_size(testFilePath);
        const auto numberOfExpectedRawBuffers
            = (fileSizeInBytes / testConfig.sizeOfRawBuffers) + static_cast<uint64_t>(fileSizeInBytes % testConfig.sizeOfRawBuffers != 0);
        return numberOfExpectedRawBuffers;
    }
    SetupResult setupTest(
        const TestConfig& testConfig,
        InputFormatterTestUtil::ThreadSafeVector<NES::Memory::TupleBuffer>& rawBuffers,
        size_t numberOfExpectedRawBuffers,
        size_t numberOfRequiredSourceBuffers)
    {
        /// Create file source, start it using the emit function, and wait for the file source to fill the result buffer vector
        std::shared_ptr<Memory::BufferManager> sourceBufferPool
            = Memory::BufferManager::create(testConfig.sizeOfRawBuffers, numberOfRequiredSourceBuffers);

        /// TODO #774: Sources sometimes need an extra buffer (reason currently unknown)
        const auto currentTestFile = testFileMap.at(testConfig.testFileName);
        const auto testFilePath = std::filesystem::path(INPUT_FORMATTER_TEST_DATA) / testConfig.formatterType / currentTestFile.fileName;
        const auto schema = InputFormatterTestUtil::createSchema(currentTestFile.schemaFieldTypes);
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
        // const auto multiplier = (testConfig.sizeOfRawBuffers * 2) >= testConfig.sizeOfFormattedBuffers ? 2 : 1;
        double multiplier = 2.0;
        if (testConfig.sizeOfRawBuffers > testConfig.sizeOfFormattedBuffers)
        {
            multiplier += (static_cast<double>(testConfig.sizeOfRawBuffers) / testConfig.sizeOfFormattedBuffers);
        }
        fmt::print("Multiplier {}\n", multiplier);
        const auto numberOfRequiredFormattedBuffers = static_cast<uint32_t>(rawBuffers.size() * multiplier);
        fmt::print("Number of formatter buffers: {}\n", numberOfRequiredFormattedBuffers);

        return SetupResult{
            .schema = std::move(schema),
            .numberOfRequiredFormattedBuffers = numberOfRequiredFormattedBuffers,
            .currentTestFileName = currentTestFile.fileName,
            .currentTestFilePath = testFilePath};
    }

    bool compareResults(
        const std::vector<std::vector<NES::Memory::TupleBuffer>>& resultBuffers,
        const TestConfig& testConfig,
        const SetupResult& setupResult) const
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

        const auto resultFilePath = std::filesystem::path(INPUT_FORMATTER_TMP_RESULT_DATA) / testConfig.formatterType
            / (std::string("result_") + setupResult.currentTestFileName);
        /// TODO #895: implement format-agnostic testing
        if (testConfig.formatterType == "Native")
        {
            const auto sizeOfTuplesInBytes = setupResult.schema.getSizeOfSchemaInBytes();
            bool append = false;
            for (const auto& buffer : resultBufferVec)
            {
                const auto numberOfBytesInBuffer = sizeOfTuplesInBytes * buffer.getNumberOfTuples();
                const std::span byteSpan(buffer.getBuffer<const char>(), numberOfBytesInBuffer);
                writeBinaryToFile(byteSpan, resultFilePath, append);
                append = true;
            }
        }
        else if (testConfig.formatterType == "CSV")
        {
            bool append = false;
            for (const auto& buffer : resultBufferVec | std::views::take(resultBufferVec.size() - 1))
            {
                auto actualResultTestBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, setupResult.schema);
                actualResultTestBuffer.setNumberOfTuples(buffer.getNumberOfTuples());
                const auto currentBufferAsString = actualResultTestBuffer.toString(
                    setupResult.schema, Memory::MemoryLayouts::TestTupleBuffer::PrintMode::NO_HEADER_END_IN_NEWLINE, testConfig.fieldDelimiter);
                writeBinaryToFile(currentBufferAsString, resultFilePath, append);
                append = true;
            }
            const auto lastBufferAsString
                = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(resultBufferVec.back(), setupResult.schema)
                      .toString(setupResult.schema, Memory::MemoryLayouts::TestTupleBuffer::PrintMode::NO_HEADER_END_IN_NEWLINE, testConfig.fieldDelimiter);
            writeBinaryToFile(lastBufferAsString, resultFilePath, append);
        }
        else
        {
            throw NotImplemented("Unsupported formatter type: {}", testConfig.formatterType);
        }
        resultBufferVec.clear();
        return InputFormatterTestUtil::compareFiles(setupResult.currentTestFilePath, resultFilePath);
    }

    void runTest(const TestConfig& testConfig)
    {
        const auto numberOfExpectedRawBuffers = getNumberOfExpectedBuffers(testConfig);
        /// Create vector for result buffers and create emit function to collect buffers from source
        InputFormatterTestUtil::ThreadSafeVector<NES::Memory::TupleBuffer> rawBuffers;
        rawBuffers.reserve(numberOfExpectedRawBuffers);

        const auto numberOfRequiredSourceBuffers = static_cast<uint32_t>(numberOfExpectedRawBuffers + 1);

        const auto setupResult = setupTest(testConfig, rawBuffers, numberOfExpectedRawBuffers, numberOfRequiredSourceBuffers);

        for (size_t i = 0; i < testConfig.numberOfIterations; ++i)
        {
            /// Prepare TestTaskQueue for processing the input formatter tasks
            auto testBufferManager
                = Memory::BufferManager::create(testConfig.sizeOfFormattedBuffers, setupResult.numberOfRequiredFormattedBuffers);
            auto inputFormatterTask = InputFormatterTestUtil::createInputFormatterTask(setupResult.schema, testConfig.formatterType, testConfig.fieldDelimiter);
            auto resultBuffers = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(testConfig.numberOfThreads);

            std::vector<TestPipelineTask> pipelineTasks;
            pipelineTasks.reserve(numberOfExpectedRawBuffers);
            rawBuffers.modifyBuffer(
                [&](auto& rawBuffers)
                {
                    for (size_t bufferIdx = 0; auto& rawBuffer : rawBuffers)
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
            if (testConfig.validate)
            {
                const auto isCorrectResult = compareResults(*resultBuffers, testConfig, setupResult);
                ASSERT_TRUE(isCorrectResult);
            }

            /// Cleanup
            resultBuffers->clear();
            testBufferManager->destroy();
        }
    }
};

TEST_F(SmallFilesTest, ysbBenchmark)
{
    /// Checked that the YSB10K produces the original/correct data with raw buffer sizes of 128 bytes
    runTest(TestConfig{
        .testFileName = "YSB10K",
        .formatterType = "CSV",
        .hasSpanningTuples = true,
        .numberOfIterations = 3,
        .numberOfThreads = 16,
        /// (on ThreadRipper) size of 256 bytes and 46 threads leads to ~50% spent in 'processSequenceNumber' (without range check!!)
        /// 16 threads and 1KB already clearly shows the sync overhead (~30%)
        .sizeOfRawBuffers = static_cast<uint64_t>(std::pow(2, 10)),
        .sizeOfFormattedBuffers = 4096,
        .fieldDelimiter = ",",
        .validate = false});
}

TEST_F(SmallFilesTest, testTwoIntegerColumns)
{
    runTest(TestConfig{
        .testFileName = "TwoIntegerColumns",
        .formatterType = "CSV",
        .hasSpanningTuples = true,
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 16,
        .sizeOfFormattedBuffers = 4096});
}

TEST_F(SmallFilesTest, testBimboData)
{
    runTest(TestConfig{
        .testFileName = "Bimbo_1_1000",
        .formatterType = "CSV",
        .hasSpanningTuples = true,
        .numberOfIterations = 10,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 16,
        .sizeOfFormattedBuffers = 4096});
}

TEST_F(SmallFilesTest, testFoodData)
{
    runTest(TestConfig{
        .testFileName = "Food_1",
        .formatterType = "CSV",
        .hasSpanningTuples = true,
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 16,
        .sizeOfFormattedBuffers = 4096});
}

TEST_F(SmallFilesTest, testSpaceCraftTelemetryData)
{
    runTest(
        {.testFileName = "Spacecraft_Telemetry",
         .formatterType = "CSV",
         .hasSpanningTuples = true,
         .numberOfIterations = 1,
         .numberOfThreads = 8,
         .sizeOfRawBuffers = 16,
         .sizeOfFormattedBuffers = 4096});
}


/// Simple test that confirms that we forward already formatted buffers without spanning tuples correctly
TEST_F(SmallFilesTest, testTwoIntegerColumnsNoSpanningBinary)
{
    runTest(TestConfig{
        .testFileName = "TwoIntegerColumns_binary",
        .formatterType = "Native",
        .hasSpanningTuples = false,
        /// Only one iteration possible, because the InputFormatterTask replaces the number of bytes with the number of tuples in a
        /// raw tuple buffer when the tuple buffer is in 'Native' format and has no spanning tuples
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 4096,
        .sizeOfFormattedBuffers = 4096});
}

}
/// NOLINTEND(readability-magic-numbers)
