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
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <memory>
#include <ostream>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configuration/WorkerConfiguration.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <FileUtil.hpp>
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
        std::vector<std::string> schemaFieldNames;
    };

    using enum InputFormatterTestUtil::TestDataTypes;
    std::unordered_map<std::string, TestFile> testFileMap{
        {"TwoIntegerColumns",
         TestFile{.fileName = "TwoIntegerColumns", .schemaFieldTypes = {INT32, INT32}, .schemaFieldNames = {"id", "value"}}},
        {"Bimbo", /// https://github.com/cwida/public_bi_benchmark/blob/master/benchmark/Bimbo/
         TestFile{
             .fileName = "Bimbo",
             .schemaFieldTypes = {INT16, INT16, INT32, INT16, FLOAT64, INT32, INT16, INT32, INT16, INT16, FLOAT64, INT16},
             .schemaFieldNames
             = {"Agencia_ID",
                "Canal_ID",
                "Cliente_ID",
                "Demanda_uni_equil",
                "Dev_proxima",
                "Dev_uni_proxima",
                "Number of Records",
                "Producto_ID",
                "Ruta_SAK",
                "Semana",
                "Venta_hoy",
                "Venta_uni_hoy"}}},
        {"Food", /// https://github.com/cwida/public_bi_benchmark/blob/master/benchmark/Food/
         TestFile{
             .fileName = "Food",
             .schemaFieldTypes = {INT16, INT32, VARSIZED, VARSIZED, INT16, FLOAT64},
             .schemaFieldNames = {"Number of Records", "activity_sec", "application", "device", "subscribers", "volume_total_bytes"}}},
        {"Spacecraft_Telemetry", /// generated
         TestFile{
             .fileName = "Spacecraft_Telemetry",
             .schemaFieldTypes = {INT32, UINT32, BOOLEAN, CHAR, VARSIZED, FLOAT32, FLOAT64},
             .schemaFieldNames
             = {"temperature_delta",
                "power_level",
                "is_sunlit",
                "status_code",
                "operation_state",
                "radiation_level",
                "orbital_velocity"}}}};

    SourceCatalog sourceCatalog;

public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("InputFormatterTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup InputFormatterTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }

    void TearDown() override { BaseUnitTest::TearDown(); }

    static bool writeBinaryToFile(const std::span<const std::ostream::char_type> data, const std::filesystem::path& filepath, bool append)
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
        std::string fileEnding;
        bool hasSpanningTuples;
        size_t numberOfIterations;
        size_t numberOfThreads;
        size_t sizeOfRawBuffers;
        bool isCompiled;
    };

    struct SetupResult
    {
        Schema schema;
        MemoryLayoutType memoryLayoutType;
        size_t sizeOfFormattedBuffers;
        size_t numberOfExpectedRawBuffers;
        size_t numberOfRequiredFormattedBuffers;
        std::string currentTestFileName;
        std::string currentTestFilePath;
    };

    size_t getNumberOfExpectedBuffers(
        const TestConfig& testConfig, const std::filesystem::path& testFilePath, USED_IN_DEBUG const size_t sizeOfSchemaInBytes) const
    {
        const auto sizeOfFormattedBuffers = WorkerConfiguration().defaultQueryExecution.operatorBufferSize.getValue();
        PRECONDITION(
            sizeOfFormattedBuffers >= sizeOfSchemaInBytes, "The formatted buffer must be large enough to hold at least one tuple.");

        const auto fileSizeInBytes = std::filesystem::file_size(testFilePath);
        const auto numberOfExpectedRawBuffers
            = (fileSizeInBytes / testConfig.sizeOfRawBuffers) + static_cast<uint64_t>(fileSizeInBytes % testConfig.sizeOfRawBuffers != 0);
        return numberOfExpectedRawBuffers;
    }

    SetupResult setupTest(const TestConfig& testConfig, InputFormatterTestUtil::ThreadSafeVector<TupleBuffer>& rawBuffers)
    {
        const auto currentTestFile = testFileMap.at(testConfig.testFileName);
        const auto schema = InputFormatterTestUtil::createSchema(currentTestFile.schemaFieldTypes, currentTestFile.schemaFieldNames);

        const auto testDirPath = std::filesystem::path(INPUT_FORMATTER_TEST_DATA) / testConfig.fileEnding;
        const auto testFilePath = [](const TestFile& currentTestFile, const std::filesystem::path& testDirPath, std::string_view fileEnding)
        {
            if (const auto testFilePath = findFileByName(currentTestFile.fileName, testDirPath))
            {
                return testFilePath.value();
            }
            throw InvalidConfigParameter(
                "Could not find file test file: {}.<file_ending_of_{}>", testDirPath / currentTestFile.fileName, fileEnding);
        }(currentTestFile, testDirPath, testConfig.fileEnding);

        const auto sizeOfFormattedBuffers = WorkerConfiguration().defaultQueryExecution.operatorBufferSize.getValue();
        const auto numberOfExpectedRawBuffers = getNumberOfExpectedBuffers(testConfig, testFilePath, schema.getSizeOfSchemaInBytes());
        rawBuffers.reserve(numberOfExpectedRawBuffers);

        /// Create file source, start it using the emit function, and wait for the file source to fill the result buffer vector
        const auto numberOfRequiredSourceBuffers = static_cast<uint16_t>(numberOfExpectedRawBuffers + 1);
        std::shared_ptr<BufferManager> sourceBufferPool = BufferManager::create(testConfig.sizeOfRawBuffers, numberOfRequiredSourceBuffers);

        /// TODO #774: Sources sometimes need an extra buffer (reason currently unknown)
        const auto [backpressureController, fileSource] = InputFormatterTestUtil::createFileSource(
            sourceCatalog, testFilePath, schema, std::move(sourceBufferPool), numberOfRequiredSourceBuffers);
        fileSource->start(InputFormatterTestUtil::getEmitFunction(rawBuffers));
        rawBuffers.waitForSize(numberOfExpectedRawBuffers);
        INVARIANT(
            fileSource->tryStop(std::chrono::milliseconds(1000)) == SourceReturnType::TryStopResult::SUCCESS, "Failed to stop source.");
        INVARIANT(
            numberOfExpectedRawBuffers == rawBuffers.size(),
            "Expected to have {} raw buffers, but got: {}",
            numberOfExpectedRawBuffers,
            rawBuffers.size());

        /// We assume that we don't need more than two times the number of buffers to represent the formatted data than we need to represent the raw data
        const auto numberOfRequiredFormattedBuffers = static_cast<uint32_t>((rawBuffers.size() + 1) * 2);

        return SetupResult{
            .schema = schema,
            .memoryLayoutType = MemoryLayoutType::ROW_LAYOUT,
            .sizeOfFormattedBuffers = sizeOfFormattedBuffers,
            .numberOfExpectedRawBuffers = numberOfExpectedRawBuffers,
            .numberOfRequiredFormattedBuffers = numberOfRequiredFormattedBuffers,
            .currentTestFileName = currentTestFile.fileName,
            .currentTestFilePath = testFilePath};
    }

    template <bool WriteExpectedResultsFile>
    bool compareResults(
        const std::vector<std::vector<TupleBuffer>>& resultBuffers,
        const SetupResult& setupResult,
        const std::vector<size_t>& varSizedFieldOffsets,
        BufferManager& testBufferManager)
    {
        /// Combine results and sort them using (ascending on sequence-/chunknumbers)
        auto combinedThreadResults = std::ranges::views::join(resultBuffers);
        std::vector<TupleBuffer> resultBufferVec(combinedThreadResults.begin(), combinedThreadResults.end());
        std::ranges::sort(
            resultBufferVec,
            [](const TupleBuffer& left, const TupleBuffer& right)
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
            writeTupleBuffersToFile(resultBufferVec, setupResult.schema, tmpExpectedResultsPath, varSizedFieldOffsets);
        }
        const auto expectedResultsPath
            = std::filesystem::path(INPUT_FORMATTER_TEST_DATA) / std::format("Expected/{}.nes", setupResult.currentTestFileName);
        auto expectedBuffers = loadTupleBuffersFromFile(testBufferManager, setupResult.schema, expectedResultsPath, varSizedFieldOffsets);
        return InputFormatterTestUtil::compareTestTupleBuffersOrderSensitive(resultBufferVec, expectedBuffers, setupResult.schema);
    }

    template <bool WriteExpectedResultsFile = false>
    void runTest(const TestConfig& testConfig)
    {
        /// Create vector for result buffers and create emit function to collect buffers from source
        InputFormatterTestUtil::ThreadSafeVector<TupleBuffer> rawBuffers;

        const auto setupResult = setupTest(testConfig, rawBuffers);

        const auto varSizedFieldOffsets = getVarSizedFieldOffsets(setupResult.schema);
        for (size_t i = 0; i < testConfig.numberOfIterations; ++i)
        {
            /// Prepare TestTaskQueue for processing the input formatter tasks
            auto testBufferManager
                = BufferManager::create(setupResult.sizeOfFormattedBuffers, setupResult.numberOfRequiredFormattedBuffers);

            /// Create compiled pipeline stage containing InputFormatter and EmitOperator(emits formatted buffers into 'resultBuffers')
            const std::unordered_map<std::string, std::string> parserConfiguration{
                {"type", testConfig.formatterType}, {"tuple_delimiter", "\n"}, {"field_delimiter", "|"}};
            auto testStage = InputFormatterTestUtil::createInputFormatter(
                parserConfiguration,
                setupResult.schema,
                setupResult.memoryLayoutType,
                setupResult.sizeOfFormattedBuffers,
                testConfig.isCompiled);

            auto resultBuffers = std::make_shared<std::vector<std::vector<TupleBuffer>>>(testConfig.numberOfThreads);
            std::vector<TestPipelineTask> pipelineTasks;
            pipelineTasks.reserve(setupResult.numberOfExpectedRawBuffers);
            rawBuffers.modifyBuffer(
                [&pipelineTasks, &testStage](const auto& buffers)
                {
                    std::ranges::for_each(
                        buffers, [&pipelineTasks, &testStage](const auto& rawBuffer) { pipelineTasks.emplace_back(rawBuffer, testStage); });
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
            NES_DEBUG("Destroyed result buffer");
        }
    }
};

TEST_F(SmallFilesTest, testTwoIntegerColumnsJSON)
{
    runTest(TestConfig{
        .testFileName = "TwoIntegerColumns",
        .formatterType = "JSON",
        .fileEnding = "JSON",
        .hasSpanningTuples = true,
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 16,
        .isCompiled = true});
}

TEST_F(SmallFilesTest, testBimboDataJSON)
{
    runTest(TestConfig{
        .testFileName = "Bimbo",
        .formatterType = "JSON",
        .fileEnding = "JSON",
        .hasSpanningTuples = true,
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 16,
        .isCompiled = true});
}

TEST_F(SmallFilesTest, testFoodDataJSON)
{
    runTest(TestConfig{
        .testFileName = "Food",
        .formatterType = "JSON",
        .fileEnding = "JSON",
        .hasSpanningTuples = true,
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 16,
        .isCompiled = true});
}

TEST_F(SmallFilesTest, testSpaceCraftTelemetryJSON)
{
    runTest(TestConfig{
        .testFileName = "Spacecraft_Telemetry",
        .formatterType = "JSON",
        .fileEnding = "JSON",
        .hasSpanningTuples = true,
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 16,
        .isCompiled = true});
}

TEST_F(SmallFilesTest, testTwoIntegerColumns)
{
    runTest(TestConfig{
        .testFileName = "TwoIntegerColumns",
        .formatterType = "CSV",
        .fileEnding = "CSV",
        .hasSpanningTuples = true,
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 16,
        .isCompiled = true});
}

TEST_F(SmallFilesTest, testBimboData)
{
    runTest(TestConfig{
        .testFileName = "Bimbo",
        .formatterType = "CSV",
        .fileEnding = "CSV",
        .hasSpanningTuples = true,
        .numberOfIterations = 1,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 2,
        .isCompiled = true});
}

TEST_F(SmallFilesTest, testFoodData)
{
    runTest(TestConfig{
        .testFileName = "Food",
        .formatterType = "CSV",
        .fileEnding = "CSV",
        .hasSpanningTuples = true,
        .numberOfIterations = 10,
        .numberOfThreads = 8,
        .sizeOfRawBuffers = 2,
        .isCompiled = true});
}

TEST_F(SmallFilesTest, testSpaceCraftTelemetryData)
{
    runTest(
        {.testFileName = "Spacecraft_Telemetry",
         .formatterType = "CSV",
         .fileEnding = "CSV",
         .hasSpanningTuples = true,
         .numberOfIterations = 10,
         .numberOfThreads = 8,
         .sizeOfRawBuffers = 2,
         .isCompiled = true});
}

}

/// NOLINTEND(readability-magic-numbers)
